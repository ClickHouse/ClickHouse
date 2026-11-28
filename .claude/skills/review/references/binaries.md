# ClickHouse binaries for reproducing findings

SCOPE
- This applies only to the CI `Code Review` job. Local reviews, including a local codex review, use the binaries their user provides and do not download CI binaries.
- A finding is stronger with a reproducer that was actually run. These binaries are available without building. Every finding that used one names the binary in its Evidence line: the job and the short commit.
- The checkout is a shallow clone of the merge of the PR into its base (`refs/remotes/pull/<N>/merge`): it has no `master`, no other branch, and none of the PR's own commits, so `git log` and `git diff` cannot reach them. Get history from GitHub with `gh api`, as in the commands below.
- Pick a binary for the machine's architecture (`uname -m`). The CI `Code Review` job runs on aarch64, so only `arm_*` binaries and the Fast test binary run there.

DISK SPACE
- The runner had 49 GB free when the job started (2026-11-26). The Fast test binary is about 100 MB; full builds are 1.4 to 2.1 GB, and a self-extracting binary needs more space when it unpacks next to itself.
- Use the Fast test binary unless the finding needs a library, a sanitizer, or an assertion it lacks.
- Before a download, compare `df -h ./ci/tmp` with the `Content-Length` from `curl -sI <url>`. Skip the download and say so in the finding when there is not enough room for it unpacked.

PR HEAD: THE FAST TEST BINARY

**Getting it**
- When the PR changes code that the Fast test depends on (`src`, `contrib`, `cmake`, `base`, `programs`, the stateless tests), the `Code Review` job starts only after `Fast test` has finished, so its binary exists. Get its URL with:
  ```bash
  node .claude/tools/fetch_ci_report.js \
    "https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?PR=<N>&sha=<head sha>&name_0=PR&name_1=Fast%20test" \
    --binary
  ```
- It prints `.../fast_test/clickhouse-stripped` when the tests passed, `.../fast_test/clickhouse` when they failed, and nothing when the build failed.
- Download it with `curl -sfL <url> -o clickhouse` and run `chmod +x clickhouse`. It is a self-extracting binary that unpacks on first run. To check it, run `./clickhouse local -q "SELECT version()"`.

**What this binary is**
- aarch64 only, built by `ci/jobs/fast_test.py`.
- `RelWithDebInfo`: no sanitizers and no debug assertions, so a `chassert` that would fail does not fire, and undefined behavior shows only when it crashes or gives a wrong result.
- `-DENABLE_LIBRARIES=0`: most optional libraries are absent (S3, Kafka, ORC, Avro, HDFS, gRPC and others). Only NuRaft, simdjson, Parquet, Rust, liburing and yaml-cpp are enabled. Features that need a missing library fail with `SUPPORT_IS_DISABLED` or an unknown name.
- Most reproducers are `clickhouse local` queries. A `clickhouse server` with the default configuration also works. Tests that need ZooKeeper, MinIO or other servers cannot run.
- "Could not reproduce with the Fast test binary" is not evidence that a finding is wrong when the code path needs a sanitizer, an assertion, or a library this build lacks. Say which one it needs.

**Reused from an earlier commit**
- When nothing that the Fast test depends on changed since an earlier run, CI does not run it again. It reuses that run, and the workflow report lists `Fast test` as `SKIPPED` with the info `reused from cache` and a link to the earlier report. The earlier commit can belong to another PR.
- `fetch_ci_report.js` follows the link, so `--binary` at the head sha prints the earlier run's binary, and its log shows `reused from cache, following <url>`.
- That binary was built from the same sources, so it is valid for the PR head, but its URL names the other commit. Name the commit the binary comes from in the Evidence line (`Fast test` of `<earlier sha>`), so readers do not take it for a stale build.

**PRs without a Fast test**
- When a PR changes only documentation or CI scripts, `Fast test` is `SKIPPED` with `Not affected by the changed files and not required`, and there is no PR binary. Its effect is the same as on master, so use the master binary (see MASTER).

FULL BUILDS: EARLIER COMMITS OF THE PR

**Which builds**
- The `Build (...)` jobs run in parallel with the `Code Review` job, so builds of the head commit are usually not ready. Earlier commits of the PR, or a build reused from cache, often have them.
- These binaries have all libraries, and the sanitizer and debug builds have the checks that the Fast test binary lacks:
  - `Build (arm_release)`, `Build (arm_binary)`: release build with all libraries.
  - `Build (arm_debug)`: debug assertions (`chassert`, `LOGICAL_ERROR` checks).
  - `Build (arm_asan_ubsan)`, `Build (arm_tsan)`, `Build (arm_msan)`: sanitizers.
- `amd_*` builds of the same jobs exist for x86-64 machines.

**Getting one**
1. List the PR's commits, oldest first: `gh api repos/<repo>/pulls/<N>/commits --paginate --jq '.[].sha'`.
2. For each commit, ask for the binary of the job:
   ```bash
   node .claude/tools/fetch_ci_report.js \
     "https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?PR=<N>&sha=<sha>&name_0=PR&name_1=Build%20(arm_debug)" \
     --binary
   ```
   The binary is `.../build_<job>/clickhouse`; ignore the packages (`.deb`, `.rpm`, `.tgz`) and `clickhouse-examples`. A job `reused from cache` prints the binary of the run it reuses, as for the Fast test.
3. Check what changed between that commit and the head: `gh api repos/<repo>/compare/<sha>...<head sha> --jq '.files[].filename'`. When the change touches the code of the finding, the binary does not show the head's behavior: use it only for parts that did not change, and say so.
- The Evidence line names the job and the commit, for example `✅ Reproduced with Build (arm_asan_ubsan) of 1a2b3c4d (differs from the head only in tests)`.

MASTER
- To show behavior before the PR, or when the PR has no binary (documentation and CI only), use a master build. Only merge commits of master have them, and not all of them:
  ```bash
  for sha in $(gh api 'repos/ClickHouse/ClickHouse/commits?sha=master&per_page=30' --jq '.[].sha')
  do
    url="https://clickhouse-builds.s3.amazonaws.com/REFs/master/$sha/masterci/build_arm_release/clickhouse"
    curl -sfI "$url" > /dev/null && { echo "$url"; break; }
  done
  ```
- Replace `build_arm_release` with another job (`build_arm_debug`, `build_arm_asan_ubsan`, ...) for other build types. A `403` means that commit has no build; the loop goes on to the previous one.
