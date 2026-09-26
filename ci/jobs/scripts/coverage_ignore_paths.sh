# Sources kept out of the LLVM coverage measurement, shared by the two scripts
# that have to agree on it: merge_llvm_coverage.sh, which passes the regex to
# `llvm-cov export -ignore-filename-regex` and so decides what ends up in
# llvm_coverage.info, and generate_diff_coverage_report.sh, which must not look
# for a changed file in a tracefile that cannot contain it.
#
# The report answers "how much of the shipped server do the tests reach", so
# anything that is not shipped server code only adds noise to the total:
#
#   contrib                 third-party code, built without instrumentation anyway
#   [/_]gtest_              the unit tests themselves. They run in the unit test
#                           shard, so they score ~97% and inflate the total with
#                           ~105k lines of test bodies rather than tested code
#   \.pb\. \.generated\.    generated sources, nobody writes tests against them
#   QueryFuzzer & co.       fuzzing helpers, exercised only by the fuzzer jobs
#                           whose profiles are not merged here
#   programs/<tool>         standalone developer and operator tools that no CI
#                           job starts, each of them at exactly 0% today. Only
#                           those: a tool the tests do run (clickhouse-disks,
#                           clickhouse-obfuscator, clickhouse-install,
#                           clickhouse-keeper-bench) is shipped code under test
#                           and stays in the report, partial coverage and all.
#
# The path this is matched against is absolute in `llvm-cov export` and
# repository-relative in the diff report, hence `(^|/)` in the last clause.
COVERAGE_IGNORE_FILENAME_REGEX='contrib'
COVERAGE_IGNORE_FILENAME_REGEX+='|[/_]gtest_'
COVERAGE_IGNORE_FILENAME_REGEX+='|\.pb\.|\.generated\.'
COVERAGE_IGNORE_FILENAME_REGEX+='|/(QueryFuzzer|ThreadFuzzer|fuzzQuery|fuzzBits|StorageFuzzQuery|hasThreadFuzzer)\.(cpp|h)$'
COVERAGE_IGNORE_FILENAME_REGEX+='|/fuzzers/'
COVERAGE_IGNORE_FILENAME_REGEX+='|(^|/)programs/(check-marks|checksum-for-compressed-block|docker-init|keeper-data-dumper|keeper-utils|su|zookeeper-dump-tree|zookeeper-remove-by-list)/'
