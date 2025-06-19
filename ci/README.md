The `./ci` directory contains praktika' CI scripts, configs, etc., They are intended to eventually replace legacy scripts from `./tests/ci`.

The `./ci/praktika` is a praktika module. It provides generic CI functionality and must not include any ClickHouse specifics.

The `./ci/workflows` is a special path for praktika from where it reads workflow definitions
The `./ci/settings` is a special path for praktika from where it reads CI settings set per project

The `./ci/jobs` directory that includes job scripts
The `./ci/docker` directory that includes docker images for clickhouse tests
The `./ci/defs` additional CI configuration files, job configs, docker configs, etc


NOTE: CI is operating currently in migration mode:
* Legacy scripts are still used for some jobs (Integration tests, Compatibility check, etc.) and they are called from praktika via command: `cd ./tests/ci && python3 ci.py --run-from-praktika`
* Configuration for legacy jobs (`./tests/ci/ci_config.py`) that are still in use must be in line with new praktika' configuration (`./ci/workflows/*`) to work properly: Job names must match
