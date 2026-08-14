"""Suite A1: every function end to end against a real model, within a bounded time.

Two kinds of assertion, marked per case:

* **Implementation-decided** - `ProfileEvents` counts, vector dimensions, cardinality, row
  alignment, JSON validity, ranges. Always asserted.
* **Model-decided** - whether the output is the right label, contains the right substring,
  obeys an instruction. The corpus is built so non-compliance is implausible, but these
  still depend on the model, so they skip on a `toy_model` target.

`NULL`/empty handling, quota behaviour and `throw_on_error` are deliberately absent: the
free mock suite (`tests/integration/test_ai_functions/`) already pins them and no endpoint can influence
them, so paying a provider to re-confirm them is waste.
"""

import json
import math

import pytest

from . import corpus as ai_corpus
from .asserts import (
    Report,
    assert_ai_usage,
    assert_within_budget,
    cosine,
    parse_json_rows,
)
from .conftest import load_table, requires_capable_model, requires_live_endpoint

pytestmark = [pytest.mark.e2e, requires_live_endpoint]

CHAT = "map('credentials','ai_e2e_chat')"
EMBED = "map('credentials','ai_e2e_embed')"
CATEGORIES = "['positive','negative','neutral']"


@pytest.fixture(scope="module")
def a1_tables(instance, cfg):
    scale = cfg.data_scale
    load_table(
        instance,
        "a1_arith",
        [("id", "UInt32"), ("text", "String"), ("answer", "String")],
        ai_corpus.arith(scale),
    )
    load_table(
        instance,
        "a1_classify",
        [("id", "UInt32"), ("text", "String"), ("label", "String")],
        ai_corpus.classify(),
    )
    load_table(
        instance,
        "a1_extract",
        [("id", "UInt32"), ("text", "String"), ("name", "String"), ("city", "String")],
        ai_corpus.extract(),
    )
    load_table(
        instance,
        "a1_translate",
        [("id", "UInt32"), ("text", "String"), ("number", "String")],
        ai_corpus.translate(),
    )
    load_table(
        instance,
        "a1_embed",
        [("id", "UInt32"), ("text", "String")],
        ai_corpus.embed_bulk(scale),
    )
    load_table(
        instance,
        "a1_pairs",
        [
            ("id", "UInt32"),
            ("anchor", "String"),
            ("paraphrase", "String"),
            ("unrelated", "String"),
        ],
        ai_corpus.embed_pairs(),
    )
    yield
    for table in (
        "a1_arith",
        "a1_classify",
        "a1_extract",
        "a1_translate",
        "a1_embed",
        "a1_pairs",
    ):
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.fixture(scope="module")
def report(cfg):
    report = Report(
        "suite_a",
        {
            "target": cfg.target.name,
            "chat_model": cfg.chat_model,
            "embed_model": cfg.embed_model,
            "scale": cfg.data_scale,
        },
    )
    yield report
    report.flush()
    print("\n" + report.render(
        ["case", "rows", "api_calls", "in_tok", "out_tok", "duration_ms", "budget_ms", "verdict"]
    ))


def _record(report, case, rows, events, budget, verdict="ok"):
    report.add(
        case,
        rows=rows,
        api_calls=events["api_calls"],
        in_tok=events["input_tokens"],
        out_tok=events["output_tokens"],
        duration_ms=events["query_duration_ms"],
        budget_ms=budget,
        verdict=verdict,
    )


# ---------------------------------------------------------------------------
# A1-1 aiGenerate
# ---------------------------------------------------------------------------


def test_a1_1_generate(q, cfg, a1_tables, report):
    rows = ai_corpus.arith(cfg.data_scale)
    result, events = q.run(
        f"SELECT id, answer, aiGenerate(text, {CHAT}) AS out FROM a1_arith "
        f"ORDER BY id FORMAT JSONEachRow",
        case="a1_1_generate",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert len(parsed) == len(rows), "cardinality must match the input"
    assert_ai_usage(
        events, "chat", len(rows), reports_token_usage=cfg.reports_token_usage
    )
    budget = assert_within_budget(events, cfg, len(rows), "chat", "a1_1")
    _record(report, "a1_1 aiGenerate/arith", len(rows), events, budget)

    if cfg.toy_model:
        pytest.skip("model-decided assertions skipped on a toy_model target")
    missing = [row for row in parsed if row["answer"] not in row["out"]]
    assert not missing, f"answer missing from output, first: {missing[:2]}"


# ---------------------------------------------------------------------------
# A1-2 aiClassify
# ---------------------------------------------------------------------------


def test_a1_2_classify(q, cfg, a1_tables, report):
    rows = ai_corpus.classify()
    result, events = q.run(
        f"SELECT id, label, aiClassify(text, {CATEGORIES}, {CHAT}) AS out "
        f"FROM a1_classify ORDER BY id FORMAT JSONEachRow",
        case="a1_2_classify",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert len(parsed) == len(rows)
    assert_ai_usage(
        events, "chat", len(rows), reports_token_usage=cfg.reports_token_usage
    )
    budget = assert_within_budget(events, cfg, len(rows), "chat", "a1_2")

    outside = [row["out"] for row in parsed if row["out"].strip() not in ai_corpus.CATEGORIES]
    wrong = [row for row in parsed if row["out"].strip() != row["label"]]
    verdict = "ok"
    if outside:
        verdict = f"{len(outside)} out-of-enum"
    _record(report, "a1_2 aiClassify", len(rows), events, budget, verdict)

    if cfg.toy_model:
        pytest.skip("model-decided assertions skipped on a toy_model target")
    # Enum membership is the user-visible contract. Note that the gateway drops
    # `response_format: json_schema`, so today only the prompt enforces it.
    assert not outside, f"output outside the category list: {outside[:3]}"
    assert not wrong, f"wrong label, first: {[(r['label'], r['out']) for r in wrong[:3]]}"


# ---------------------------------------------------------------------------
# A1-3 / A1-4 aiExtract
# ---------------------------------------------------------------------------


def test_a1_3_extract_schema(q, cfg, a1_tables, report):
    rows = ai_corpus.extract()
    result, events = q.run(
        f"SELECT id, name, city, "
        f"aiExtract(text, '{ai_corpus.EXTRACT_SCHEMA}', {CHAT}) AS out "
        f"FROM a1_extract ORDER BY id FORMAT JSONEachRow",
        case="a1_3_extract",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert len(parsed) == len(rows)
    assert_ai_usage(
        events, "chat", len(rows), reports_token_usage=cfg.reports_token_usage
    )
    budget = assert_within_budget(events, cfg, len(rows), "chat", "a1_3")

    invalid = [row["out"] for row in parsed if not _is_json_object(row["out"])]
    verdict = "ok" if not invalid else f"{len(invalid)} non-JSON"
    _record(report, "a1_3 aiExtract/schema", len(rows), events, budget, verdict)

    if cfg.toy_model:
        pytest.skip("model-decided assertions skipped on a toy_model target")
    # Model-decided: the gateway drops `response_format: json_schema` and `aiExtract` does
    # not validate the body, so only the prompt makes this JSON.
    assert not invalid, f"output is not a JSON object: {invalid[:2]}"
    for row in parsed:
        payload = json.loads(row["out"])
        assert set(payload) == {"name", "city"}, (
            f"key set {sorted(payload)} - the schema asked for name and city only, and "
            "age/order are decoys that must not appear"
        )
        assert row["name"].lower() in str(payload["name"]).lower()
        assert row["city"].lower() in str(payload["city"]).lower()


def test_a1_4_extract_instruction(q, cfg, a1_tables, report):
    rows = ai_corpus.extract()
    result, events = q.run(
        f"SELECT id, city, "
        f"aiExtract(text, '{ai_corpus.EXTRACT_INSTRUCTION}', {CHAT}) AS out "
        f"FROM a1_extract ORDER BY id FORMAT JSONEachRow",
        case="a1_4_extract_instruction",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert_ai_usage(
        events, "chat", len(rows), reports_token_usage=cfg.reports_token_usage
    )
    budget = assert_within_budget(events, cfg, len(rows), "chat", "a1_4")
    _record(report, "a1_4 aiExtract/instruction", len(rows), events, budget)

    assert all(row["out"].strip() for row in parsed), "empty output"
    if cfg.toy_model:
        pytest.skip("model-decided assertions skipped on a toy_model target")
    missing = [row for row in parsed if row["city"].lower() not in row["out"].lower()]
    assert not missing, f"city missing from output: {missing[:2]}"


# ---------------------------------------------------------------------------
# A1-5 aiTranslate
# ---------------------------------------------------------------------------


def test_a1_5_translate(q, cfg, a1_tables, report):
    rows = ai_corpus.translate()
    result, events = q.run(
        f"SELECT id, text, number, "
        f"aiTranslate(text, '{ai_corpus.TRANSLATE_TARGET}', {CHAT}) AS out "
        f"FROM a1_translate ORDER BY id FORMAT JSONEachRow",
        case="a1_5_translate",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert_ai_usage(
        events, "chat", len(rows), reports_token_usage=cfg.reports_token_usage
    )
    budget = assert_within_budget(events, cfg, len(rows), "chat", "a1_5")
    _record(report, "a1_5 aiTranslate", len(rows), events, budget)

    assert all(row["out"].strip() for row in parsed), "empty translation"
    if cfg.toy_model:
        pytest.skip("model-decided assertions skipped on a toy_model target")
    unchanged = [row for row in parsed if row["out"].strip() == row["text"].strip()]
    assert not unchanged, f"output identical to the source: {unchanged[:2]}"
    lost = [row for row in parsed if row["number"] not in row["out"]]
    assert not lost, f"order number lost in translation: {lost[:2]}"


# ---------------------------------------------------------------------------
# A1-6 aiEmbed
# ---------------------------------------------------------------------------


def test_a1_6_embed(q, cfg, a1_tables, report):
    rows = ai_corpus.embed_bulk(cfg.data_scale)
    result, events = q.run(
        f"SELECT id, aiEmbed(text, '{cfg.embed_model}', {EMBED}) AS vec "
        f"FROM a1_embed ORDER BY id FORMAT JSONEachRow",
        case="a1_6_embed",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert len(parsed) == len(rows)
    assert_ai_usage(
        events,
        "embed",
        len(rows),
        batch=cfg.embed_batch_size,
        reports_token_usage=cfg.reports_token_usage,
    )
    budget = assert_within_budget(events, cfg, len(rows), "embed", "a1_6")

    dimensions = {len(row["vec"]) for row in parsed}
    assert len(dimensions) == 1, f"inconsistent vector dimension: {sorted(dimensions)}"
    native = dimensions.pop()
    assert native > 0, "empty embedding"
    for row in parsed:
        assert all(math.isfinite(value) for value in row["vec"]), "non-finite component"
        norm = math.sqrt(sum(value * value for value in row["vec"]))
        assert norm > 0, "zero-norm embedding"
    _record(
        report,
        "a1_6 aiEmbed/bulk",
        len(rows),
        events,
        budget,
        f"native dim {native}",
    )


# ---------------------------------------------------------------------------
# A1-7 aiSimilarity
# ---------------------------------------------------------------------------


def test_a1_7_similarity(q, cfg, a1_tables, report):
    rows = ai_corpus.embed_pairs()
    model = cfg.embed_model
    result, events = q.run(
        f"SELECT id, "
        f"aiSimilarity(anchor, paraphrase, '{model}', {EMBED}) AS close, "
        f"aiSimilarity(anchor, unrelated, '{model}', {EMBED}) AS far, "
        f"aiSimilarity(anchor, anchor, '{model}', {EMBED}) AS self "
        f"FROM a1_pairs ORDER BY id FORMAT JSONEachRow",
        case="a1_7_similarity",
        counting=True,
        rows=len(rows),
    )
    parsed = parse_json_rows(result)
    assert len(parsed) == len(rows)
    # Three `aiSimilarity` expressions, each its own function instance, each batching both
    # operands of every live row: ceil(2 * live_rows / batch) calls per expression.
    per_expression = math.ceil(2 * len(rows) / cfg.embed_batch_size)
    assert events["api_calls"] == 3 * per_expression, (
        f"AIAPICalls={events['api_calls']}, expected {3 * per_expression} "
        f"(3 expressions x ceil(2 x {len(rows)} operands / {cfg.embed_batch_size}))"
    )
    budget = assert_within_budget(events, cfg, len(rows), "embed", "a1_7")
    _record(report, "a1_7 aiSimilarity", len(rows), events, budget)

    for row in parsed:
        for key in ("close", "far", "self"):
            assert row[key] is not None, f"{key} is NULL for row {row['id']}"
            assert -1.001 <= row[key] <= 1.001, f"{key}={row[key]} out of range"
        assert row["self"] >= 0.999, (
            f"identical strings scored {row['self']}, expected ~1.0"
        )

    if cfg.toy_model:
        pytest.skip("model-decided assertions skipped on a toy_model target")
    bad = [row for row in parsed if row["close"] <= row["far"] + 0.05]
    assert not bad, (
        "paraphrase must score above unrelated by a margin, failing rows: "
        f"{[(r['id'], r['close'], r['far']) for r in bad]}"
    )


# ---------------------------------------------------------------------------
# A1-8 all six functions in one query
# ---------------------------------------------------------------------------


def test_a1_8_all_functions_one_query(q, cfg, instance, report):
    rows = 4
    mixed = [
        {
            "id": index,
            "gen": arith["text"],
            "answer": arith["answer"],
            "cls": classify["text"],
            "label": classify["label"],
            "ext": extract["text"],
            "city": extract["city"],
            "tr": translate["text"],
            "number": translate["number"],
        }
        for index, (arith, classify, extract, translate) in enumerate(
            zip(
                ai_corpus.arith()[:rows],
                ai_corpus.classify()[:rows],
                ai_corpus.extract()[:rows],
                ai_corpus.translate()[:rows],
            )
        )
    ]
    load_table(
        instance,
        "a1_mixed",
        [
            ("id", "UInt32"),
            ("gen", "String"),
            ("answer", "String"),
            ("cls", "String"),
            ("label", "String"),
            ("ext", "String"),
            ("city", "String"),
            ("tr", "String"),
            ("number", "String"),
        ],
        mixed,
    )
    try:
        model = cfg.embed_model
        result, events = q.run(
            f"SELECT id, answer, label, city, number, "
            f"aiGenerate(gen, {CHAT}) AS gen_out, "
            f"aiClassify(cls, {CATEGORIES}, {CHAT}) AS cls_out, "
            f"aiExtract(ext, '{ai_corpus.EXTRACT_SCHEMA}', {CHAT}) AS ext_out, "
            f"aiTranslate(tr, '{ai_corpus.TRANSLATE_TARGET}', {CHAT}) AS tr_out, "
            f"aiEmbed(cls, '{model}', {EMBED}) AS emb_out, "
            f"aiSimilarity(cls, ext, '{model}', {EMBED}) AS sim_out "
            f"FROM a1_mixed ORDER BY id FORMAT JSONEachRow",
            case="a1_8_all",
            counting=True,
            rows=rows,
        )
        parsed = parse_json_rows(result)
        assert len(parsed) == rows

        # Four text functions issue one request per row; `aiEmbed` batches its 4 texts into
        # one request, and `aiSimilarity` batches both operands of all rows into one more.
        expected_calls = 4 * rows + 1 + 1
        assert events["api_calls"] == expected_calls, (
            f"AIAPICalls={events['api_calls']}, expected {expected_calls} "
            "(4 text functions per row, plus one embedding batch each for aiEmbed and "
            "aiSimilarity)"
        )
        budget = assert_within_budget(events, cfg, 4 * rows, "chat", "a1_8")
        _record(report, "a1_8 all six functions", rows, events, budget)

        for row in parsed:
            assert row["gen_out"].strip(), "empty aiGenerate output"
            assert _is_json_object(row["ext_out"]), "aiExtract did not return an object"
            assert row["tr_out"].strip(), "empty translation"
            assert len(row["emb_out"]) > 0, "empty embedding"
            assert row["sim_out"] is not None
            assert -1.001 <= row["sim_out"] <= 1.001

        if cfg.toy_model:
            pytest.skip("model-decided assertions skipped on a toy_model target")
        for row in parsed:
            assert row["answer"] in row["gen_out"]
            assert row["cls_out"].strip() in ai_corpus.CATEGORIES
    finally:
        instance.query("DROP TABLE IF EXISTS a1_mixed SYNC")


def _is_json_object(text):
    try:
        return isinstance(json.loads(text), dict)
    except ValueError:
        return False
