-- The `allow_get_client_http_header` permission is enforced when `getClientHTTPHeader` is built
-- for a scope. A scope with the setting disabled must throw `FUNCTION_NOT_ALLOWED` even when a
-- sibling scope already resolved an identical call with it enabled: sharing the resolved
-- `FunctionBase` across scopes through the analyzer function cache would be a permission bypass.

SET enable_analyzer = 1;

-- Baseline: allowed scope works (empty result over the TCP interface, which has no headers).
SELECT (SELECT getClientHTTPHeader('X-Test') SETTINGS allow_get_client_http_header = 1);

-- The disabled sibling must throw even though the enabled scope resolved the same call first.
SELECT
    (SELECT getClientHTTPHeader('X-Test') SETTINGS allow_get_client_http_header = 1),
    (SELECT getClientHTTPHeader('X-Test') SETTINGS allow_get_client_http_header = 0); -- { serverError FUNCTION_NOT_ALLOWED }

-- Same, with the disabled scope first.
SELECT
    (SELECT getClientHTTPHeader('X-Test') SETTINGS allow_get_client_http_header = 0),
    (SELECT getClientHTTPHeader('X-Test') SETTINGS allow_get_client_http_header = 1); -- { serverError FUNCTION_NOT_ALLOWED }
