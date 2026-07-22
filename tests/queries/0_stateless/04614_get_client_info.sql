-- getClientInfo exposes ClientInfo fields for the current query (issue #18408).

SELECT getClientInfo('query_kind') IN ('INITIAL_QUERY', 'SECONDARY_QUERY', 'NO_QUERY');
SELECT getClientInfo('interface') IN ('TCP', 'LOCAL', 'HTTP');
SELECT getClientInfo('current_user') = currentUser();
SELECT (getClientInfo('initial_user') = currentUser()) OR (getClientInfo('initial_user') = '');
SELECT length(getClientInfo('client_hostname')) >= 0;
SELECT length(getClientInfo('client_name')) >= 0;
SELECT getClientInfo('client_version_major') >= 0;
SELECT getClientInfo('client_version_minor') >= 0;
SELECT getClientInfo('client_version_patch') >= 0;
SELECT getClientInfo('is_secure') IN (0, 1);
SELECT getClientInfo('distributed_depth') >= 0;
SELECT getClientInfo('http_method') IN ('UNKNOWN', 'GET', 'POST', 'OPTIONS');

SELECT toTypeName(getClientInfo('client_hostname'));
SELECT toTypeName(getClientInfo('client_version_major'));
SELECT toTypeName(getClientInfo('client_tcp_protocol_version'));
SELECT toTypeName(getClientInfo('is_secure'));
SELECT toTypeName(getClientInfo('initial_query_start_time'));
SELECT toTypeName(getClientInfo('initial_query_start_time_microseconds'));

-- Unknown attribute
SELECT getClientInfo('no_such_attribute'); -- { serverError BAD_ARGUMENTS }

-- Argument must be a constant string
SELECT getClientInfo(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- DEFAULT / MATERIALIZED column expressions
DROP TABLE IF EXISTS t_get_client_info;
CREATE TABLE t_get_client_info
(
    x UInt32,
    host String DEFAULT getClientInfo('client_hostname'),
    iface String MATERIALIZED getClientInfo('interface')
)
ENGINE = Memory;

INSERT INTO t_get_client_info (x) VALUES (1);
SELECT x, host = getClientInfo('client_hostname'), iface IN ('TCP', 'LOCAL', 'HTTP') FROM t_get_client_info;

DROP TABLE t_get_client_info;
