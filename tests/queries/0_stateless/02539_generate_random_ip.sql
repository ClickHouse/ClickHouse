-- Check that the function works for Ipv4 and Ipv6 and gives at least something plausible:
SELECT uniq(v4) > 1000, uniq(v6) > 1000 FROM (SELECT * FROM generateRandom('v4 IPv4, v6 IPv6') LIMIT 100000);
