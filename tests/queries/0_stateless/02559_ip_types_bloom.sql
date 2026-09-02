DROP TABLE IF EXISTS ip_bloom;

CREATE TABLE ip_bloom
(
    `a` UInt32,
    `ip4` Nullable(IPv4),
    `ip6` Nullable(IPv6),
    INDEX x4 ip4 TYPE bloom_filter(0.1) GRANULARITY 3,
    INDEX x6 ip6 TYPE bloom_filter(0.1) GRANULARITY 3
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO ip_bloom VALUES (1, '1.1.1.1', '::1');

SELECT * FROM ip_bloom;

DROP TABLE ip_bloom;
