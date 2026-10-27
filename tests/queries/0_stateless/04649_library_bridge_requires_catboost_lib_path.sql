-- `clickhouse-library-bridge` loads shared libraries with `dlopen`, so it must never be started without a
-- `--libraries-path` sandbox. `catboost_lib_path` is the only path it is allowed to load from, and it is not
-- configured here, so reaching the bridge through `system.models` has to fail instead of starting it unrestricted.
-- The second code covers an environment where `catboost_lib_path` is configured but the bridge is unavailable.
SELECT * FROM system.models; -- { serverError NO_ELEMENTS_IN_CONFIG, EXTERNAL_SERVER_IS_NOT_RESPONDING }
