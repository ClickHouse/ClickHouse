-- verify that we populate system.dns_cache with dns info for localhost addresses
SELECT * FROM system.dns_cache ORDER BY hostname;
