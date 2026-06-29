# ClickHouse CI System

This directory contains the CI (Continuous Integration) system for ClickHouse based on the `praktika` framework. The CI system is responsible for automated building, testing, and deployment of ClickHouse.

## Directory Structure

- `./ci/praktika/` - Core praktika module that provides generic CI functionality. This module is designed to be reusable and must not include any ClickHouse-specific logic.

- `./ci/workflows/` - Contains workflow definitions that praktika uses to orchestrate CI jobs. This is a special path that praktika recognizes automatically.

- `./ci/settings/` - Contains project-specific CI settings. This is another special path that praktika recognizes automatically. Settings defined here override default praktika's settings in ./ci/praktika/settings.py::_Settings

- `./ci/jobs/` - Contains individual job scripts that define the specific tasks to be executed.

- `./ci/docker/` - Contains Dockerfiles and related resources for building container images used in ClickHouse tests.

- `./ci/defs/` - Contains additional CI configuration files, including job configurations, Docker configurations, and other definitions.
