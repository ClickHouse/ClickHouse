# Native Jobs

This file defines special "native" jobs that are built into the Praktika CI framework. These jobs handle critical workflow operations like configuration, Docker image building, and finalization.

## Overview

Native jobs are Praktika's system-level jobs that run at key points in the CI workflow lifecycle:

1. **Config Workflow** (`_config_workflow`) - Runs at workflow start to configure the entire CI run
2. **Docker Build Jobs** (`_build_dockers`) - Build Docker images for AMD, ARM, and multi-platform manifests
3. **Finish Workflow** (`_finish_workflow`) - Runs at workflow end to collect results and set final status

These jobs are distinguished from user-defined jobs and are automatically included in every workflow when enabled via workflow settings.

## Job Configurations

The file defines several job configurations:
- `_workflow_config_job` - Configuration job that runs on `CI_CONFIG_RUNS_ON` runners
- `_docker_build_manifest_job` - Merges AMD/ARM manifests into multi-platform images
- `_docker_build_amd_linux_job` - Builds AMD64 Docker images
- `_docker_build_arm_linux_job` - Builds ARM64 Docker images
- `_final_job` - Finalization job that runs even if workflow is cancelled

## Config Workflow (`_config_workflow`)

The configuration job performs critical setup at the start of each workflow run:

### Main Steps

1. **Repository Setup**
   - Unshallows git repository to ensure full history is available
   - Fetches base branch for PR comparisons
   - Extracts commit messages and PR metadata

2. **GitHub Integration**
   - Authenticates with GitHub API
   - Refreshes PR title, body, and labels
   - Posts pending status to commit and PR comments
   - Sets up workflow status reporting

3. **Pre-flight Checks**
   - Validates YAML workflow files are up to date
   - Verifies secrets configuration (commented out to avoid throttling)
   - Checks CI database connectivity if enabled
   - Runs user-defined pre-hooks

4. **Docker Digest Calculation**
   - Calculates content digests for all Docker images in the workflow
   - Stores digests in `RunConfig` for cache and build decisions

5. **Job Filtering**
   - Applies custom workflow filter hooks if defined
   - **Filters jobs based on changed files** (see `check_affected_jobs` below)
   - Marks unaffected jobs as skipped to optimize CI runtime

6. **Cache Configuration**
   - Looks up previous successful runs in cache
   - Configures jobs to reuse cached results when possible
   - Stores cache mappings in `RunConfig`

7. **Report Initialization**
   - Sets up HTML report structure
   - Pushes pending report to S3
   - Initializes status tracking

### Job Filtering: `check_affected_jobs`

This function implements intelligent job skipping based on file changes, significantly reducing CI runtime for PRs that only touch specific parts of the codebase.

#### Logic Flow

```
┌─────────────────────────────────────┐
│ Get changed files from PR/commit    │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ Find affected Docker images         │
│ (Docker images that need rebuilding)│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│ For each job (excluding native Praktika jobs):     │
│                                                      │
│ Check if job is affected by:                        │
│   1. Requires artifacts from affected jobs?         │
│   2. Runs in an affected Docker image?              │
│   3. Directly affected by changed files?            │
│      (via job.is_affected_by(changed_files))        │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ If AFFECTED:                        │
│ - Track job's artifacts as affected │
│ - Track job's required artifacts    │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ If NOT AFFECTED:                    │
│ - Jobs with no artifacts → SKIP     │
│ - Jobs with artifacts → DEFER       │
│   (may still be needed)             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│ Second pass: Unaffected jobs with artifacts         │
│                                                      │
│ For each deferred job:                              │
│   - If artifacts are in all_required_artifacts      │
│     → CANNOT SKIP (other jobs need these artifacts) │
│   - If artifacts not required anywhere              │
│     → SKIP (not affected and not needed)            │
└─────────────────────────────────────────────────────┘
```

#### Key Concepts

**Affected Jobs**: A job is considered affected if:
- It requires artifacts produced by an affected job
- It runs in a Docker image that was modified
- Its file dependency patterns match changed files (via `Job.is_affected_by`)

**Artifact Dependency Chain**: The algorithm tracks artifact dependencies to ensure that if Job B requires artifacts from Job A, and Job A is affected, then Job B must also run (even if Job B's code wasn't changed).

**Ordering-Only Dependencies**: When a job requirement matches a job name (rather than an artifact name), it's treated as an ordering-only dependency. Job names in requirements are excluded from `all_required_artifacts` to prevent keeping alive unnecessary jobs. If you need artifacts from a job, reference the artifact name directly, not the job name.

**Deferred Decision**: Jobs that produce artifacts but aren't directly affected cannot be immediately skipped - they're only skipped if no other affected job requires their artifacts.

#### Example Scenario

```
Changed files: src/database/query_parser.cpp

Jobs:
- build_parser (provides: parser_binary, runs in: builder_image)
- build_server (requires: parser_binary, provides: server_binary)
- test_ui (requires: server_binary)
- build_docs (provides: documentation)

Result:
1. builder_image is affected (contains src/database/)
2. build_parser is AFFECTED (runs in affected Docker image)
   - Adds "parser_binary" to affected_artifacts
3. build_server is AFFECTED (requires affected artifact "parser_binary")
   - Adds "server_binary" to affected_artifacts
4. test_ui is AFFECTED (requires affected artifact "server_binary")
5. build_docs is SKIPPED (not affected, no one needs "documentation")
```

**Example: Ordering-Only Dependencies**

```
Changed files: tests/integration/test_new_feature.py

Jobs:
- Job D (test_new_feature, directly affected)
  - requires: ["Job C"] (job name → ordering dependency)
- Job C (setup_test_env, provides: nothing)
  - requires: ["artifact_b"] (artifact name → data dependency)
- Job B (build_artifact, provides: ["artifact_b"])

Result:
1. Job D is AFFECTED (directly by changed files)
   - Job D requires "Job C" (matches a job name)
   - Job names indicate ordering-only dependencies
   - "Job C" is NOT added to all_required_artifacts
2. Job C is SKIPPED (not affected, provides nothing)
3. Job B is SKIPPED (not affected, "artifact_b" not in all_required_artifacts)
   - Only artifact names (not job names) are tracked in all_required_artifacts
   - Job C's requirement "artifact_b" was never added because Job C is not affected
```

#### Implementation Details

- **Native jobs are never filtered**: Jobs identified by `_is_praktika_job` are always run
- **Graceful degradation**: If changed files cannot be determined, no jobs are filtered (safe default)
- **Artifact name aliasing**: When a job provides artifacts, its job name is also added to `affected_artifacts` for cases where artifact reports reference job names instead of artifact names
- **Ordering-only dependency detection**: When building `all_required_artifacts` from affected jobs, the algorithm checks if each requirement matches a job name. If it does, the requirement is excluded from `all_required_artifacts` as job names indicate ordering-only dependencies. Only artifact names are added to track actual data dependencies
- **Configuration storage**: Filtered jobs are marked in `workflow_config.filtered_jobs` with the reason for skipping

### Output

The function returns a `Result` object containing:
- Status of all configuration steps
- List of files to be uploaded (workflow config, report structure)
- Any errors encountered during configuration

The workflow configuration is persisted to filesystem via `RunConfig.dump()` and used by all subsequent jobs in the workflow.

## Finish Workflow (`_finish_workflow`)

[Placeholder: This job runs at the end of the workflow to collect all job results, check for failures, post final status to GitHub, run post-hooks, and set the "Ready for Merge" status if enabled.]
