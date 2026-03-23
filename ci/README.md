# ClickHouse CI System

This directory contains the CI (Continuous Integration) system for ClickHouse based on the `praktika` framework. The CI system is responsible for automated building, testing, and deployment of ClickHouse.

## Directory Structure

- `./ci/praktika/` - Core praktika module that provides generic CI functionality. This module is designed to be reusable and must not include any ClickHouse-specific logic.

- `./ci/workflows/` - Contains workflow definitions that praktika uses to orchestrate CI jobs. This is a special path that praktika recognizes automatically.

- `./ci/settings/` - Contains project-specific CI settings. This is another special path that praktika recognizes automatically.

- `./ci/jobs/` - Contains individual job scripts that define the specific tasks to be executed.

- `./ci/docker/` - Contains Dockerfiles and related resources for building container images used in ClickHouse tests.

- `./ci/defs/` - Contains additional CI configuration files, including job configurations, Docker configurations, and other definitions.

## Migration Status

> **IMPORTANT**: The CI system is currently in migration mode.

The new praktika-based system is gradually replacing the legacy CI scripts in `./tests/ci/`. During this transition:

1. Legacy scripts are still used for some jobs (e.g., Integration tests, Compatibility checks) and are invoked from praktika using the command:
   ```
   cd ./tests/ci && python3 ci.py --run-from-praktika
   ```

2. Configuration consistency is critical - job names in the legacy configuration (`./tests/ci/ci_config.py`) must match exactly with the new praktika configuration (`./ci/workflows/*`) for proper operation.

## Getting Started

# TODO
To run CI jobs locally or understand how to interact with the system, see the documentation in `./ci/praktika/docs/` or consult the online documentation.