import subprocess
from types import SimpleNamespace

from ci.jobs import parser_memory_check


def test_master_profiler_url_uses_clickhouse_examples(monkeypatch):
    sha = "a" * 40
    checked_urls = []
    monkeypatch.setattr(
        parser_memory_check,
        "Info",
        lambda: SimpleNamespace(get_kv_data=lambda key: [sha]),
    )
    monkeypatch.setattr(
        parser_memory_check.Shell,
        "check",
        lambda command: checked_urls.append(command) or True,
    )

    url = parser_memory_check.get_merge_base_profiler_url()

    assert url.endswith(f"/REFs/master/{sha}/build_arm_release/clickhouse-examples")
    assert url in checked_urls[0]


def test_profiler_uses_clickhouse_examples_multicall(tmp_path, monkeypatch):
    heap_before = tmp_path / "before.heap"
    heap_after = tmp_path / "after.heap"
    heap_before.touch()
    heap_after.touch()
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            args,
            0,
            stdout="8\t100\t124\t24\n",
            stderr=(f"Profile before: {heap_before}\n" f"Profile after: {heap_after}\n"),
        )

    monkeypatch.setattr(parser_memory_check.subprocess, "run", fake_run)

    result = parser_memory_check.run_profiler_collect_heap(
        "/tmp/clickhouse-examples", "SELECT 1", str(tmp_path / "profile")
    )

    assert result["error"] is None
    assert result["jemalloc_diff"] == 24
    assert calls[0][0] == [
        "/tmp/clickhouse-examples",
        "parser_memory_profiler",
        "--profile",
        str(tmp_path / "profile"),
    ]


def test_profiler_rejects_malformed_tsv(monkeypatch):
    monkeypatch.setattr(
        parser_memory_check.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args, 0, stdout="not-tsv\n", stderr=""
        ),
    )

    result = parser_memory_check.run_profiler_collect_heap(
        "/tmp/clickhouse-examples", "SELECT 1", "/tmp/profile"
    )

    assert result == {
        "error": "malformed profiler output: expected 4 TSV fields, got 1"
    }


def test_batch_symbolize_uses_clickhouse_examples_multicall(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="symbolized\n")

    monkeypatch.setattr(parser_memory_check.subprocess, "run", fake_run)

    assert parser_memory_check.batch_symbolize(
        "/tmp/clickhouse-examples", ["before.heap", "after.heap"]
    )
    assert calls[0][0] == [
        "/tmp/clickhouse-examples",
        "parser_memory_profiler",
        "--symbolize-batch",
        "before.heap",
        "after.heap",
    ]


def test_canonical_stack_correlates_debug_and_stripped_symbols():
    master_frames = [
        "DB::ParserSelectWithUnionQuery::parseImpl--boost::intrusive_ptr<DB::ASTSelectWithUnionQuery> DB::make_intrusive<DB::ASTSelectWithUnionQuery>()",
        "DB::IParserBase::parse--DB::IParserBase::wrapParseImpl--operator()",
        "DB::ParserQueryWithOutput::parseImpl.llvm.123456",
    ]
    pr_frames = [
        "DB::ParserSelectWithUnionQuery::parseImpl",
        "DB::IParserBase::parse",
        "DB::ParserQueryWithOutput::parseImpl.llvm.987654",
    ]

    assert parser_memory_check.canonicalize_stack_frames(
        master_frames
    ) == parser_memory_check.canonicalize_stack_frames(pr_frames)


def test_cross_version_diff_correlates_same_allocation_stack():
    master_frames = [
        "DB::ParserSelectWithUnionQuery::parseImpl--boost::intrusive_ptr<DB::ASTSelectWithUnionQuery> DB::make_intrusive<DB::ASTSelectWithUnionQuery>()",
        "DB::IParserBase::parse--DB::IParserBase::wrapParseImpl--operator()",
    ]
    pr_frames = [
        "DB::ParserSelectWithUnionQuery::parseImpl",
        "DB::IParserBase::parse",
    ]
    master_stacks = [
        (
            192,
            "master display",
            parser_memory_check.flatten_frames_full(master_frames),
            parser_memory_check.canonicalize_stack_frames(master_frames),
        )
    ]
    pr_stacks = [
        (
            192,
            "PR display",
            parser_memory_check.flatten_frames_full(pr_frames),
            parser_memory_check.canonicalize_stack_frames(pr_frames),
        )
    ]

    assert (
        parser_memory_check.compute_cross_version_diff(master_stacks, pr_stacks)
        == []
    )


def test_cross_version_diff_correlates_boost_container_allocation_prefix():
    master_frames = [
        "boost::container::vec_iterator<boost::intrusive_ptr<DB::IAST>*, false> "
        "boost::container::vector<boost::intrusive_ptr<DB::IAST>>::"
        "priv_insert_forward_range_no_capacity<boost::container::dtl::"
        "insert_emplace_proxy<boost::container::new_allocator<boost::intrusive_ptr<DB::IAST>>, "
        "boost::intrusive_ptr<DB::ASTLiteral>>>(boost::intrusive_ptr<DB::IAST>*, unsigned long)",
        "boost::container::vector_alloc_holder<boost::container::new_allocator<"
        "boost::intrusive_ptr<DB::IAST>>, unsigned int>::allocate(unsigned long)",
        "boost::container::allocator_traits<boost::container::new_allocator<"
        "boost::intrusive_ptr<DB::IAST>>>::allocate("
        "boost::container::new_allocator<boost::intrusive_ptr<DB::IAST>>&, unsigned long)",
        "boost::container::new_allocator<boost::intrusive_ptr<DB::IAST>>::allocate(unsigned long)",
        "boost::intrusive_ptr<DB::IAST>* boost::container::dtl::"
        "operator_new_allocate<boost::intrusive_ptr<DB::IAST>>(unsigned long)",
        "DB::Layer::mergeElement(bool)",
        "DB::FunctionLayer::parse(DB::IParser::Pos&, DB::Expected&, DB::Action&)",
    ]
    pr_frames = [
        "boost::container::vec_iterator<boost::intrusive_ptr<DB::IAST>*, false> "
        "boost::container::vector<boost::intrusive_ptr<DB::IAST>>::"
        "priv_insert_forward_range_no_capacity<boost::container::dtl::"
        "insert_emplace_proxy<boost::container::new_allocator<boost::intrusive_ptr<DB::IAST>>, "
        "boost::intrusive_ptr<DB::IAST>>>(boost::intrusive_ptr<DB::IAST>*, unsigned long)",
        "DB::Layer::mergeElement(bool)",
        "DB::FunctionLayer::parse(DB::IParser::Pos&, DB::Expected&, DB::Action&)",
    ]

    master_stacks = [
        (
            48,
            "master display",
            parser_memory_check.flatten_frames_full(master_frames),
            parser_memory_check.canonicalize_stack_frames(master_frames),
        )
    ]
    pr_stacks = [
        (
            48,
            "PR display",
            parser_memory_check.flatten_frames_full(pr_frames),
            parser_memory_check.canonicalize_stack_frames(pr_frames),
        )
    ]

    assert (
        parser_memory_check.compute_cross_version_diff(master_stacks, pr_stacks)
        == []
    )
    assert parser_memory_check.build_cross_version_diff_flamegraph_inputs(
        master_stacks, pr_stacks
    ) == ([], [])


def test_cross_version_diff_keeps_distinct_callers_after_allocation_prefix():
    allocation_frame = (
        "boost::container::vec_iterator<boost::intrusive_ptr<DB::IAST>*, false> "
        "boost::container::vector<boost::intrusive_ptr<DB::IAST>>::"
        "priv_insert_forward_range_no_capacity<boost::container::dtl::"
        "insert_emplace_proxy<boost::container::new_allocator<boost::intrusive_ptr<DB::IAST>>, "
        "boost::intrusive_ptr<DB::IAST>>>(boost::intrusive_ptr<DB::IAST>*, unsigned long)"
    )
    master_canonical = parser_memory_check.canonicalize_stack_frames(
        [allocation_frame, "DB::Layer::mergeElement(bool)"]
    )
    pr_canonical = parser_memory_check.canonicalize_stack_frames(
        [allocation_frame, "DB::ASTSelectQuery::setExpression(Expression, ASTPtr&&)"]
    )

    assert master_canonical != pr_canonical


def test_cross_version_diff_flamegraph_uses_canonical_stacks():
    master_stacks = [
        (
            48,
            "master display",
            ["master full frame"],
            ["DB::Layer::mergeElement(bool)", "DB::ParserExpression::parseImpl()"],
        )
    ]
    pr_stacks = [
        (
            48,
            "PR display",
            ["PR full frame"],
            ["DB::ASTSelectQuery::setExpression()", "DB::ParserExpression::parseImpl()"],
        )
    ]

    master_collapsed, pr_collapsed = (
        parser_memory_check.build_cross_version_diff_flamegraph_inputs(
            master_stacks, pr_stacks
        )
    )

    assert master_collapsed == [
        (
            "DB::ParserExpression::parseImpl();DB::Layer::mergeElement(bool)",
            48,
        )
    ]
    assert pr_collapsed == [
        (
            "DB::ParserExpression::parseImpl();DB::ASTSelectQuery::setExpression()",
            48,
        )
    ]


def test_canonical_stack_keeps_allocation_prefix_without_clickhouse_caller():
    frames = [
        "boost::container::new_allocator<int>::allocate(unsigned long)",
        "third_party::buildVector()",
    ]

    assert parser_memory_check.canonicalize_stack_frames(frames) == frames


def test_compute_diff_excludes_dump_profile_allocations():
    stacks_after = {
        "stack": {
            "bytes": 192,
            "frames": [
                "std::__1::basic_string<char>::append",
                "(anonymous namespace)::dumpProfile",
                "mainEntryExampleParserMemoryProfiler",
            ],
        }
    }

    assert parser_memory_check.compute_diff({}, stacks_after) == (0, [])


def test_main_stops_after_batch_symbolization_failure(tmp_path, monkeypatch):
    (tmp_path / "clickhouse-examples").touch()
    completed_results = []

    monkeypatch.setattr(parser_memory_check, "TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(parser_memory_check, "load_queries", lambda _: ["SELECT 1"])
    monkeypatch.setattr(
        parser_memory_check, "get_merge_base_profiler_url", lambda: "master-url"
    )
    monkeypatch.setattr(
        parser_memory_check, "download_master_binary", lambda *_: ""
    )
    monkeypatch.setattr(parser_memory_check.Shell, "check", lambda _: True)
    monkeypatch.setattr(
        parser_memory_check,
        "run_profiler_collect_heap",
        lambda *_: {
            "error": None,
            "heap_before": str(tmp_path / "before.heap"),
            "heap_after": str(tmp_path / "after.heap"),
        },
    )
    monkeypatch.setattr(parser_memory_check, "batch_symbolize", lambda *_: False)

    def unexpected_call(*_args, **_kwargs):
        raise AssertionError("analysis and report generation must not run")

    monkeypatch.setattr(
        parser_memory_check,
        "analyze_heap_profiles",
        unexpected_call,
    )
    monkeypatch.setattr(
        parser_memory_check,
        "generate_html_report",
        unexpected_call,
    )

    class CompletedResult:
        def complete_job(self):
            return None

    def fake_create_from(**kwargs):
        completed_results.append(kwargs["results"])
        return CompletedResult()

    monkeypatch.setattr(
        parser_memory_check.Result,
        "create_from",
        staticmethod(fake_create_from),
    )

    parser_memory_check.main()

    assert len(completed_results) == 1
    assert completed_results[0][-1].name == "Batch symbolization"
    assert completed_results[0][-1].status == parser_memory_check.Result.Status.FAIL
