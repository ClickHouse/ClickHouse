The `./ci` directory contains refactored scripts from the legacy CI `./tests/ci` and is intended to eventually replace it.
The `./ci/docker` directory includes refactored versions of the legacy test Docker images `./dockers` and is also planned to replace them.
The new Pull Request CI workflow is defined in `./ci/workflow/pull_request.py`. Currently, it operates in "migration mode," executing legacy jobs from `./tests/ci`.
The new Master CI workflow is defined in `./ci/workflow/master.py`. Currently, it also runs in "migration mode," executing legacy jobs from `./tests/ci`.
Once all major legacy workflows are replaced with the new ones, legacy jobs can gradually be replaced by their refactored versions from `./ci/jobs/`, which are independent of CI infrastructure and can be run locally.