SELECT * FROM url('http://localhost:8123/ping', CSV, 'auto', headers());
SELECT hostname, ip_address, ip_family, (isNotNull(cached_at) AND cached_at > '1970-01-01 00:00:00') FROM system.dns_cache WHERE hostname = 'localhost';
