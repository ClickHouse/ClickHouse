-- Tags: no-darwin
-- no-darwin: NetworkTCPSockets and related metrics are populated from /proc/net on Linux only.
SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'NetworkTCPSockets';
SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'NetworkTCPSockets_LISTEN';
SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'NetworkTCPSocketRemoteAddresses';
