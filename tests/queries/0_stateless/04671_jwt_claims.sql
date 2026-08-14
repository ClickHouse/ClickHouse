-- Tags: no-fasttest
-- Tag no-fasttest: `contrib/jwt-cpp` is not in the fast test's submodule list

-- A `.` in a numeric token in JWT `CLAIMS` used to abort the server with SIGILL: picojson read the
-- decimal separator with `localeconv`, which `base/harmful` bans. Reproduces only on amd64 debug
-- and sanitizer builds, the only ones that link `harmful` and compile its traps.

DROP USER IF EXISTS user_04671;

CREATE USER user_04671 IDENTIFIED WITH jwt CLAIMS '{"a":1.5}';
SHOW CREATE USER user_04671;

CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '{"nested":{"b":-0.25},"arr":[1.5,2.5]}';
SHOW CREATE USER user_04671;

CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '{"a":1.5e-3}';
SHOW CREATE USER user_04671;

CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '10.0.0.1'; -- { serverError BAD_ARGUMENTS }
CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '3.5'; -- { serverError BAD_ARGUMENTS }

-- Content after a complete object must be rejected, not silently ignored.
CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '{"role":"admin"} AND {"tenant":"acme"}'; -- { serverError BAD_ARGUMENTS }
CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '{"a":1.5} trailing'; -- { serverError BAD_ARGUMENTS }
CREATE USER OR REPLACE user_04671 IDENTIFIED WITH jwt CLAIMS '{"a":1.5}  ';

DROP USER user_04671;
