SELECT hostname, ip_address, ip_family, cached_at FROM system.dns_cache
LIMIT 0
FORMAT TSVWithNamesAndTypes;
