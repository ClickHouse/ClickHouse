"""Deterministic corpora for the AI-function end-to-end suite.

Two flavors, per README.md:

* **Pinned rows** back semantic assertions. Fixed text, verbatim, and they do NOT scale:
  duplicating the same twelve sentences buys no coverage and only costs money.
* **Loop rows** back counting, timing and concurrency assertions. They scale with
  `AI_E2E_DATA_SCALE` and carry a unique ` (ref NNNNNN)` suffix so no two prompts are
  byte-identical, which defeats endpoint-side response caching.

Every corpus is a list of dicts with an `id` key, ordered, and identical between runs.
"""

REF_TEMPLATE = " (ref {:06d})"


def _ref(index):
    return REF_TEMPLATE.format(index)


# ---------------------------------------------------------------------------
# arith - loop rows. The checkable part is the exact sum, which any usable model
# produces; it is still a model-decided assertion and skips on a `toy_model` target.
# ---------------------------------------------------------------------------

ARITH_BASE = 8


def arith(scale=1):
    rows = []
    for i in range(ARITH_BASE * scale):
        left = 100 + 3 * i
        right = 11 + 7 * i
        rows.append(
            {
                "id": i,
                "text": (
                    f"What is {left} + {right}? Reply with the number only." + _ref(i)
                ),
                "answer": str(left + right),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# classify - pinned rows. Sentences are deliberately unambiguous so that a
# correct-label assertion is about the plumbing, not about model quality.
# ---------------------------------------------------------------------------

CATEGORIES = ["positive", "negative", "neutral"]

_CLASSIFY = [
    ("I love this product, it works perfectly and I am delighted.", "positive"),
    ("Absolutely fantastic experience, everything exceeded my hopes.", "positive"),
    ("This is wonderful news and it made my whole week better.", "positive"),
    ("Great quality and it arrived earlier than promised.", "positive"),
    ("This is terrible, it broke on the first day and I am angry.", "negative"),
    ("Awful service, nobody replied and I wasted my money.", "negative"),
    ("The worst purchase I have ever made, completely useless.", "negative"),
    ("I am very disappointed, the item arrived damaged and dirty.", "negative"),
    ("The meeting is scheduled for Tuesday at ten in the morning.", "neutral"),
    ("The package contains three cables and one power adapter.", "neutral"),
    ("The office is located on the fourth floor of the building.", "neutral"),
    ("The report lists the total number of rows in each table.", "neutral"),
]


def classify():
    return [
        {"id": i, "text": text, "label": label}
        for i, (text, label) in enumerate(_CLASSIFY)
    ]


# ---------------------------------------------------------------------------
# extract - pinned rows. `age` and the order number are decoys: they must not
# appear in the output key set, which is what makes the schema assertion sharp.
# ---------------------------------------------------------------------------

EXTRACT_SCHEMA = '{"name": "the person name", "city": "the city they live in"}'
EXTRACT_INSTRUCTION = "Extract the city name"

_EXTRACT = [
    ("Alice", "Berlin", 30, 41001),
    ("Bruno", "Lisbon", 41, 41002),
    ("Chiara", "Milan", 27, 41003),
    ("Diego", "Madrid", 52, 41004),
    ("Elena", "Prague", 34, 41005),
    ("Farid", "Cairo", 45, 41006),
    ("Greta", "Vienna", 29, 41007),
    ("Hiro", "Osaka", 38, 41008),
]


def extract():
    return [
        {
            "id": i,
            "text": (
                f"{name} lives in {city} and is {age} years old. Order {order}."
            ),
            "name": name,
            "city": city,
            "age": age,
            "order": order,
        }
        for i, (name, city, age, order) in enumerate(_EXTRACT)
    ]


# ---------------------------------------------------------------------------
# translate - pinned rows. The order number survives translation, so it is the
# deterministic part; the rest of the sentence is not asserted on.
# ---------------------------------------------------------------------------

TRANSLATE_TARGET = "French"

_TRANSLATE_ORDERS = [51001, 51002, 51003, 51004, 51005, 51006, 51007, 51008]


def translate():
    return [
        {
            "id": i,
            "text": f"Order {order} shipped to Berlin on Monday.",
            "number": str(order),
        }
        for i, order in enumerate(_TRANSLATE_ORDERS)
    ]


# ---------------------------------------------------------------------------
# embed_pairs - pinned triplets. The assertion is an ordering, not a value:
# cos(anchor, paraphrase) must beat cos(anchor, unrelated) by a margin.
# ---------------------------------------------------------------------------

_EMBED_TRIPLETS = [
    (
        "The cat sat on the warm windowsill.",
        "A feline rested on the sunny window ledge.",
        "Quarterly revenue grew by twelve percent.",
    ),
    (
        "How do I reset my account password?",
        "What is the procedure for changing my login password?",
        "The volcano erupted for the third time this century.",
    ),
    (
        "This database stores data in columns.",
        "The storage engine is column oriented.",
        "She baked a lemon cake for the party.",
    ),
    (
        "The train to Munich departs at noon.",
        "There is a midday departure to Munich by rail.",
        "Photosynthesis converts light into chemical energy.",
    ),
    (
        "Please send the invoice by email.",
        "Kindly forward the bill electronically.",
        "The mountain range spans four countries.",
    ),
    (
        "The server returned an authentication error.",
        "Login failed because the credentials were rejected.",
        "He plays the trumpet in a jazz quartet.",
    ),
]


def embed_pairs():
    return [
        {"id": i, "anchor": anchor, "paraphrase": paraphrase, "unrelated": unrelated}
        for i, (anchor, paraphrase, unrelated) in enumerate(_EMBED_TRIPLETS)
    ]


# ---------------------------------------------------------------------------
# embed_bulk - loop rows, distinct by construction.
# ---------------------------------------------------------------------------

EMBED_BULK_BASE = 40

_BULK_SUBJECTS = [
    "the merge tree",
    "a distributed query",
    "the query planner",
    "a materialized view",
    "the compression codec",
    "an aggregate function",
    "the primary key index",
    "a background merge",
]

_BULK_PREDICATES = [
    "handles sparse data efficiently",
    "was measured under load",
    "reduces disk usage",
    "improves scan throughput",
    "was documented last week",
]


def embed_bulk(scale=1):
    rows = []
    total = EMBED_BULK_BASE * scale
    for i in range(total):
        subject = _BULK_SUBJECTS[i % len(_BULK_SUBJECTS)]
        predicate = _BULK_PREDICATES[(i // len(_BULK_SUBJECTS)) % len(_BULK_PREDICATES)]
        rows.append({"id": i, "text": f"Row {i}: {subject} {predicate}." + _ref(i)})
    return rows


# ---------------------------------------------------------------------------
# Rows for the mock-driven structural and latency work. These never reach a real
# provider, so they neither scale with cost nor need semantic content.
# ---------------------------------------------------------------------------


def mock_rows(count, distinct_values=None):
    """`count` rows of short text, keyed `x` to match the mock tables' column name.

    With `distinct_values` set, the text cycles through that many distinct strings, which
    is what the dedup and memoization scenarios need.
    """
    rows = []
    for i in range(count):
        if distinct_values:
            text = f"sample text number {i % distinct_values}"
        else:
            text = f"sample text number {i}"
        rows.append({"id": i, "x": text})
    return rows


def all_live_texts(scale=1):
    """Every string the live suites send, for the pre-run spend estimate."""
    texts = [row["text"] for row in arith(scale)]
    texts += [row["text"] for row in classify()]
    texts += [row["text"] for row in extract()]
    texts += [row["text"] for row in translate()]
    texts += [row["text"] for row in embed_bulk(scale)]
    for row in embed_pairs():
        texts += [row["anchor"], row["paraphrase"], row["unrelated"]]
    return texts
