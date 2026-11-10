# Rust Avro decoder

`ENABLE_AVRO_RUST` is `ON` in every build that has Rust (`ENABLE_RUST`) and Avro (`ENABLE_AVRO`), except FreeBSD. It selects the Rust reader for `Avro`, `AvroConfluent`, Iceberg/Paimon metadata and Hudi layout-2 replacement instants. Builds without Rust keep the `contrib/avro` decoder. The choice is made at build time; there is no runtime setting. The encoder still uses `contrib/avro`.

For an independent human or AI performance review:

1. On a dedicated Linux benchmark host, build this same revision twice using identical compiler, optimization and dependency options. Set `-DENABLE_AVRO_RUST=OFF` for one build and `ON` for the other. Verify `USE_AVRO_RUST` in each build's `includes/configs/config.h`. Do not benchmark on a laptop.
2. Start isolated servers with separate data directories on TCP ports 9000 (C++) and 19000 (Rust). In the performance CI image, or an environment with the runner's Python dependencies installed, run:

   ```sh
   mkdir -p tmp
   python3 tests/performance/scripts/perf.py \
       tests/performance/avro_decoder.xml \
       --host localhost localhost --port 9000 19000 --runs 11 \
       > tmp/avro_decoder_ab.tsv 2> tmp/avro_decoder_ab.log
   ```

   The suite sets each of the six codecs on `CREATE TABLE`, not `INSERT`. Verify the generated OCF `avro.codec` headers before making codec-specific claims. Existing Avro cases in `count_from_formats`, `formats_hits_read`, `formats_columns_*`, `parse_engine_file` and `select_format` add other workloads. See `tests/performance/scripts/README.md` for report generation and CPU/allocation profiles.
3. Preserve per-query timings, errors, peak memory, binary revisions/options and codec headers. Report regressions and unstable queries, not just aggregate wins. Static review identified unmeasured risks worth testing: 10k tiny Avro files with narrow/wide schemas at 1/16 threads; cold Iceberg manifest reads; and large Iceberg manifest lists and delete manifests, where Rust paths can materialize more data than legacy streaming readers.

Hot paths: `src/exec.rs` and `src/ffi.rs` in this crate, and `src/Processors/Formats/Impl/Avro/{AvroRustPlan,AvroOcfReader}.cpp`. Fuzzer targets are in `src/Processors/Formats/fuzzers/`, with corpora in `tests/fuzz/avro_*_fuzzer.in/`.

An x86 Linux comparison (77 queries of `avro_decoder.xml` and the existing Avro tests) measured a geometric-mean Rust / `contrib/avro` median ratio of 0.619, with no query slower beyond the harness threshold.
