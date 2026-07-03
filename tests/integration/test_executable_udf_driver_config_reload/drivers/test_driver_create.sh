#!/usr/bin/env bash
# Minimal driver `create_command` used only by the integration test to observe that the
# experimental gate `allow_experimental_executable_udf_drivers` and the driver registry are
# refreshed by `SYSTEM RELOAD CONFIG`. It deliberately fails with a recognizable marker on
# stderr so the test can distinguish "the driver was actually invoked" (UDF_EXECUTION_FAILED,
# which requires both the gate to be open and the registry to have loaded the driver) from
# "the feature is disabled" (SUPPORT_IS_DISABLED) - without compiling or running anything.
echo "TEST_DRIVER_INVOKED" >&2
exit 1
