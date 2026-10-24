# Plan: AWS access from the runner host only

This is a plan, not an audit: the table comes from a grep of `ci/` at `master` on 2026-10-06, and it does not read each job in depth. The instance metadata settings of the Linux runner pools are defined outside this repository, so the plan lists them as open questions instead of facts.

## The problem in general terms {#the-problem}

A CI job runs code that the author of a pull request controls. The job runs in a container, but some containers get the cloud credentials of the host machine.

A process that holds credentials keeps them in memory, in its environment and sometimes in files. A crash dump, a log line or an uploaded artifact can copy them out.

The credentials of the host have write access to buckets that other jobs and users read. A person with a copy of them can replace a built binary, a cache entry or a report. Downstream jobs and users then consume the replaced object as if CI produced it.

Together: one leak in a container that the pull request controls can become a substituted release artifact.

## Terms {#terms}

- **Host**: the EC2 runner machine. The `praktika` runner process runs on it.
- **Job container**: the container that `praktika` starts from `Job.Config.run_in_docker`, see `ci/praktika/runner.py`.
- **IMDS**: the EC2 instance metadata service at `169.254.169.254`. It gives the instance role credentials to any process that can reach it.
- **AWS access**: any call signed with AWS credentials: S3 read or write, SSM Parameter Store, Secrets Manager, STS, KMS, ECR. Anonymous HTTPS downloads from public buckets are not AWS access.
- **Lock**: one independent control that stops an attack by itself. The target state has at least two locks for each risk.

## How a job container gets AWS credentials today {#how-credentials-reach-containers}

1. **Host network.** `--network=host` gives the container the network stack of the host, so it reaches IMDS with the default hop limit of 1. `ci/defs/job_configs.py` says so for the fast test: `--network=host required for ec2 metadata http endpoint to work`.
2. **Bridge network with hop limit 2 or more.** A container on the default bridge reaches IMDS if the launch template sets `HttpPutResponseHopLimit` to 2 or more. The Linux pool templates are not in this repository (open question 1).
3. **Host Docker socket.** The integration and Keeper stress containers mount `/run` or `/var/run` from the host. The socket accepts writes on a read-only mount, so the container can start a new container with `--network=host` and reach IMDS.
4. **`--privileged`.** A privileged container can mount host devices and leave the container. A privileged container is the host for this threat model.
5. **Environment variables.** `sign_macos_binary.py` assumes `release_signing` in the container and exports `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` to all child processes.

The `praktika` pre-run (artifact download) and post-run (artifact and report upload, `CIDB` insert) already run on the host. Jobs without `run_in_docker` run their own AWS calls on the host, but the AST fuzzer, BuzzHouse and stress jobs start containers that read SSM themselves (see the tables below).

## Credentials in a job container {#credentials}

`praktika` gives a job container no AWS credentials. The container gets credentials only when its own code fetches them.

1. **What `docker run` passes.** `ci/praktika/runner.py` passes no `AWS_*` variable and does not mount `~/.aws`. The container gets the checkout, the staged `praktika` package, the `+-e` and `+--volume` settings from `run_in_docker`, `~/.config/gh` for jobs with `enable_gh_auth`, and `--env-file ci/local.env` when that file exists.
2. **GitHub secrets.** The generated workflow exports each GitHub secret of the job into the environment of the host step (`TEMPLATE_SETUP_ENV_SECRETS` in `ci/praktika/yaml_generator.py`). A secret reaches the container only through `+-e NAME` in `run_in_docker`. No AWS key is a GitHub secret.
3. **AWS secrets.** A `Secret.Config` of type `AWS_SSM_PARAMETER` or `AWS_SSM_SECRET` is not resolved in advance. `Secret.get_value` calls boto3 in the process that asks for the value, which is in the container for a containerized job.
4. **The boto3 credential chain in the container.** boto3 and the AWS CLI try environment variables, then `~/.aws/config` and `~/.aws/credentials`, then IMDS. CI sets no variables and the images contain no `~/.aws`, so IMDS is the only source. The container therefore holds credentials exactly when it can reach IMDS, see the paths above.
5. **What IMDS returns.** Temporary STS credentials of the instance role of the pool: an access key, a secret key and a session token. They expire after some hours, and the SDK fetches new ones by itself. They carry all permissions of the role, not the permissions of the job.
6. **Credentials made inside a job.** A job can exchange the IMDS credentials for other credentials and export them. `sign_macos_binary.py` calls `aws sts assume-role` for `release_signing` and puts the result into `AWS_*` variables, which every child process inherits.
7. **Local runs.** `ci/local.env.example` suggests personal `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for a private `sccache` bucket. `praktika` loads `ci/local.env` into its own environment and passes the file to `docker run --env-file`, so long-lived personal keys go into the container. `praktika` also loads the file in CI when it exists in the checkout (to check if a PR can add it).

## Credential access by job {#jobs-table}

The tables cover the 64 distinct jobs of all workflows, read from `_get_workflows`, and every call site of `Secret.get_value`, boto3, the AWS CLI, signed `S3` methods and `GHAuth` that each job reaches. "IMDS" in the network column means the container reaches IMDS on any pool. "Bridge" means it reaches IMDS only if the hop limit is 2 or more (open question 1). Secret names are SSM parameters unless stated otherwise.

### In the job container that `praktika` starts {#in-job-container}

| Job | Runs on PRs | Network | Credential | What for | Proposed move |
|---|---|---|---|---|---|
| Build (`build_clickhouse.py`) | yes, and merge queue | host, IMDS | instance role through `sccache` and `clang-tidy` cache | S3 cache in `clickhouse-builds`: read on PRs, read and write on `master` and release branches | `sccache` server or a signing cache proxy on the host |
| Fast test (`fast_test.py`) | yes, and merge queue | host, IMDS | instance role through `sccache` | S3 cache read | as Build |
| Bugfix validation, unit tests (`unit_tests_bugfix_validation_job.py`) | yes | host, IMDS | instance role through `sccache` | S3 cache read | as Build |
| Build toolchain (`build_toolchain.py`) | yes | host, IMDS | none called; the role is reachable | none | drop host network |
| Collect profiles (`collect_clickhouse_profiles.py`) | no | host, IMDS | none called; the role is reachable | none | drop host network |
| Stateless tests (`functional_tests.py`) | yes, and merge queue | bridge | `clickhouse-test-stat-connection`; `clickhouse_ci_logs_host` and `clickhouse_ci_logs_password` | `CIDB` reads for test targeting (`find_tests.py`) and `CIDB` writes for per-test coverage; server log export to the CI logs cluster (`log_export.py`) | host resolves targeting in pre-run; log cluster gets a per-job write-only token |
| Bugfix validation, functional (`functional_tests.py`) | yes | host, IMDS | as Stateless tests | as Stateless tests | as Stateless tests; drop host network |
| Integration tests (`integration_test_job.py`) | yes | bridge, host Docker socket | `clickhouse-test-stat-connection` | `CIDB` reads for test targeting and for test durations to balance batches (`integration_tests_configs.py`), `CIDB` writes for coverage | host computes the test list and batches in pre-run; remove the socket mount |
| Performance comparison (`performance_tests.py`) | yes | bridge | `clickhouse-test-stat-connection`; CI logs cluster secrets | dashboard table uploads to `CIDB`; server log export | uploads in a host post-hook; per-job log token |
| ClickBench (`clickbench.py`) | yes | host, IMDS | CI logs cluster secrets | server log export | per-job log token; drop host network |
| SQLStorm (`sqlstorm_test.py`) | yes | bridge | CI logs cluster secrets | server log export | per-job log token |
| Build profile diff (`build_profile_diff_job.py`) | yes | bridge, `gh` mount | CI logs cluster secrets (`LogCluster`); GitHub App token | reads of build profiles; `gh api` calls | host post-hook for the reads |
| LLVM coverage (`llvm_coverage_job.py`) | yes | bridge, `gh` mount | GitHub App token | `gh api` and `gh pr` calls | keep; limit the token permissions |
| Style check (`check_style.py`) | yes, and merge queue | bridge, `gh` mount | GitHub App token | mounted by `enable_gh_auth`; no call found in the script | remove the mount |
| PromQL compliance (`promql_compliance_job.py`) | yes | bridge, `gh` mount | GitHub App token | mounted by `enable_gh_auth`; no call found in the script | remove the mount |
| Sign macOS binary (`sign_macos_binary.py`) | no, release branches | host, IMDS | role `release_runner`, then STS `release_signing` in `AWS_*` variables; `/release/apple-notary/notary_key` | KMS signing through PKCS#11; Apple notarization | signing and notarization on the host, only of the declared artifact after a digest check |

No credential call in the container: unit tests, Keeper stress (has the host Docker socket), docs check, docs examples, `SQLLogic` test, `SQLTest`, SQLancer, SQLancerPP, parser and storage memory checks, WebAssembly parser build, vector search stress. Their downloads are anonymous HTTPS. Their containers still need IMDS blocked (plan step 4).

### In containers that a host job starts itself {#in-inner-container}

| Job | Runs on PRs | Container | Credential | What for |
|---|---|---|---|---|
| AST fuzzer, BuzzHouse (`ast_fuzzer_job.py`, `run-fuzzer.sh`) | yes | `--network=host`, `--privileged`, IMDS | CI logs cluster secrets | server log export (`clickhouse_proc.py logs_export_config`) |
| Stress test (`stress_job.py`, `stress_runner.sh`) | yes | bridge | CI logs cluster secrets | server log export |
| Jepsen (`jepsen_check.py`) | no | `--network=host`, IMDS | SSH agent socket with `jepsen_ssh_key` | SSH to the Jepsen cluster |

Upgrade check, `libFuzzer`, install check and compatibility check start containers that use no credentials.

### On the host {#on-host}

| Where | Credential | What for |
|---|---|---|
| `praktika` runner, every job | instance role; `clickhouse-test-stat-connection` | artifact and report S3 transfers in pre-run and post-run, `CIDB` insert of results |
| `praktika` runner, `enable_gh_auth` jobs | instance role invokes the token minter Lambda (`GHAuth`) | GitHub App installation token, written to `~/.config/gh` and then mounted into the container |
| `Config Workflow` and its hooks (`filter_job.py`, `version_log.py`) | instance role; `clickhouse-test-stat-connection` | workflow cache in S3, test targeting from `CIDB` |
| `Dockers Build`, Docker server and keeper images | `clickhouse-dockerhub-registry`, `dockerhub_robot_password` | image push |
| Post-hooks (`build_master_head_hook.py`, `promql_compliance_*_hook.py`, `llvm_coverage_hook.py`, `build_profile_hook.py`, `ingest_keeper_metrics.py`) | instance role; `clickhouse-test-stat-connection` | S3 uploads, `CIDB` inserts |
| Release jobs (`release_job.py`, `release_branch_job.py`, `auto_release_job.py`) | GitHub token, Docker Hub, R2 test and production, GPG signing key | release publication; `release_job.py` writes the R2 keys to `~/.r2_auth*` |
| Code review, changelog, revert of CI regressions | OpenAI keys, GitHub App token, `clickhouse-test-stat-connection` | AI review and reports |
| Statistics, `libFuzzer` corpus, `clickhousectl` upload | instance role; `clickhouse-test-stat-connection` | S3 uploads, `CIDB` reads |
| Jepsen | instance role (boto3 `autoscaling`, `ec2`); `jepsen_ssh_key` | scale the Jepsen cluster; SSH |

## Risks and their locks {#risks}

1. **A crash dump contains credentials.** A core of the server, `sccache`, Python or the AWS CLI holds the keys in memory. Locks: (a) no credentials in any container process; (b) IMDS blocked from containers, so a process cannot fetch keys later; (c) cores encrypted with `ci/defs/public.pem` before upload, see `ci/decrypt-cores.md`; (d) short session duration of the instance role. Lock (c) alone is weak: the encryption code comes from the PR checkout, the core is on the runner disk unencrypted before encryption, and the holders of `private-cores.pem` can read the keys.
2. **Leaked credentials replace a built binary.** A writer to `clickhouse-builds` can replace a binary that a later job, the install script or a user downloads. Locks: (a) the `untrusted_runner` role has no write access to build and cache prefixes; (b) PR and trusted jobs never share a runner pool or a role; (c) the host records the SHA-256 of each artifact in a store that the artifact writer role cannot change, and consumers check it; (d) release artifacts are signed, and S3 versioning or Object Lock is on for release prefixes. `SCCACHE_S3_RW_MODE=READ_ONLY` in `build_clickhouse.py` is not a lock: the PR can delete the line. IAM must give the same result.
3. **Cache poisoning.** A job that writes to `sccache`, `clang-tidy-cache` or Docker images changes the output of later trusted builds. Locks: as in risk 2, plus separate cache prefixes for each trust level and a cache key that includes the toolchain digest.
4. **The host process runs PR code.** `_job_python_env` in `ci/praktika/runner.py` puts the checkout (`.`) on `PYTHONPATH`, and the runner runs `pre_hooks` and `post_hooks` from the job configuration of the checkout. A hostile pull request can add a hook and read every credential that the host holds. Until this stops, a move out of the container stops leaks from crashes, but not from a hostile pull request. Locks: (a) the host loads `praktika` from the staged package and the job configuration and hooks from the base branch (plan step 2); (b) PR runners get a role that is safe to leak.
5. **Secrets in logs and files.** `praktika` prints the full `docker run` command. A secret given with `-e` appears in the log. `sign_macos_binary.py` writes the notary key to `ci/tmp`, and `release_job.py` writes `~/.r2_auth`. Locks: reusable secrets stay on the host (plan step 6); artifacts never include `ci/tmp` as a whole; the log masks known secret values.
6. **Runners are reused.** If a runner is not ephemeral, a job can leave a process or a modified image for the next job. Locks: ephemeral runners (open question 2); image pull by digest.
7. **The instance role is too wide.** One instance profile serves all jobs on a pool. Locks: one role for each trust level; session policies for each job, made on the host by `praktika` with `sts:AssumeRole`.

## Plan {#plan}

Steps 2 and 3 are prerequisites. Before step 2, a hostile pull request reads the host credentials through a host hook. Before step 3, a privileged container or a container with the host Docker socket goes around the IMDS block of step 4.

1. **Verify the facts.** On each pool, run `curl -X PUT http://169.254.169.254/latest/api/token` from a bridge container and from a `--network=host` container. Record the hop limit, the role and its policy. Confirm which roles can write to `clickhouse-builds`.
2. **Stop running PR code on the host.** The host loads the job configuration and the hooks from the base branch, and host processes do not get the checkout on `PYTHONPATH`.
3. **Remove the escape paths from containers.** Remove the host Docker socket from the integration and Keeper stress containers: they already run their own daemon (`docker_in_docker.sh`). Replace `--privileged` with the capabilities that each job needs (stateless tests, bugfix validation, integration tests, Keeper stress, unit tests, fuzzers). Where a job must stay privileged, run it on a pool whose role is safe to leak.
4. **Block IMDS from containers.** Set hop limit 1 on all launch templates. Add an `iptables` rule in the `DOCKER-USER` chain that drops traffic to `169.254.169.254` from Docker bridges. Remove `--network=host` from jobs that need it only for IMDS. This is a second lock only for containers without `--privileged` and without the host Docker socket, so step 3 comes first.
5. **Move S3 transfers to the host.** Job scripts write outputs to `ci/tmp` and declare them as `praktika` artifacts. Inputs come from `requires`. The host does all signed S3 calls in pre-run and post-run.
6. **Keep reusable secrets on the host.** A secret that enters the container can leave it through a log or an artifact, also from a read-only mount. So the host does the operation itself, in pre-run, in a post-hook or through a host proxy: test targeting, `CIDB` inserts, dashboard uploads. Where the container must call a service that CI owns (log export to the CI logs cluster), the host issues a token for that job only: write-only, limited to the tables of the job, and expiring with the job.
7. **Run caches on the host.** Run the `sccache` server on the host with the role credentials. The container connects through a socket and has no credentials. Alternative: a host-local signing proxy, as `ci/praktika/infrastructure/native/s3_proxy_user_data.sh` does for reports.
8. **Sign on the host, not through the container.** A socket to the KMS key lets the container sign any payload. So the host signs: it takes the binary that the job declares in `requires`, checks its digest against the digest recorded by the build (step 9), signs it with `release_signing` and KMS, and notarizes it. The container gets no signing interface.
9. **Add the artifact integrity lock.** The host records the digest of each artifact in a store that the artifact writer role cannot change: GitHub Checks, `CIDB` with separate credentials, signed metadata, or an S3 prefix with Object Lock. A digest in the job result on S3 is not enough, because the role that writes the artifact also writes the result. Consumers check the digest.
10. **Enforce.** Make `praktika` refuse, unless the job is on an allow list: `--network=host`, a host socket mount, `--privileged`, `-e` or `--env` of `AWS_*` or another credential name, `--env-file`, and mounts of `~/.aws` or `~/.config/gh`. Load `ci/local.env` only in local runs.

## Open questions {#open-questions}

1. What is `HttpPutResponseHopLimit` for each Linux pool? The stateless tests read SSM from a bridge container. Either the hop limit is 2 or more, or these reads fail.
2. Are all Linux runners ephemeral?
3. Which roles can write to `clickhouse-builds`, and are PR and `master` jobs on separate roles?
