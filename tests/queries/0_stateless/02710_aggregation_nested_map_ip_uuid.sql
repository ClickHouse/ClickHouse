DROP TABLE IF EXISTS summing_table;
CREATE TABLE summing_table
(
    id UInt32,
    `ip4Map.value` Array(IPv4), `ip4Map.total` Array(UInt32),
    `ip6Map.value` Array(IPv6), `ip6Map.total` Array(UInt32),
    `uuidMap.value` Array(UUID), `uuidMap.total` Array(UInt32)
) ENGINE = SummingMergeTree ORDER BY id;

INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values (1, ['1.2.3.4'], [1], ['::1'], [2], ['00130949-0cd4-4c3d-84c4-cc421eff480f'], [3]);
INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(1, ['1.2.3.4'], [4], ['::1'], [5], ['00130949-0cd4-4c3d-84c4-cc421eff480f'], [6]);
OPTIMIZE TABLE summing_table FINAL;
SELECT * FROM summing_table ORDER BY id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total;

INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(2, ['1.2.3.4'], [7], ['::1'], [8], ['00130949-0cd4-4c3d-84c4-cc421eff480f'], [9]);
INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(1, ['1.2.3.4'], [10], ['::1'], [11], ['00130949-0cd4-4c3d-84c4-cc421eff480f'], [12]);
INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(1, ['2.3.4.5'], [13], ['::2'], [14], ['00000000-0cd4-4c3d-84c4-cc421eff480f'], [15]);
INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(2, ['2.3.4.5'], [16], ['::1'], [17], ['00130949-0cd4-4c3d-84c4-cc421eff480f'], [18]);
INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(2, ['1.2.3.4'], [19], ['::2'], [20], ['00130949-0cd4-4c3d-84c4-cc421eff480f'], [21]);
INSERT INTO summing_table(id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total)
    values(1, ['1.2.3.4'], [22], ['::1'], [23], ['00000000-0cd4-4c3d-84c4-cc421eff480f'], [24]);
OPTIMIZE TABLE summing_table FINAL;
SELECT * FROM summing_table ORDER BY id, ip4Map.value, ip4Map.total, ip6Map.value, ip6Map.total, uuidMap.value, uuidMap.total;

DROP TABLE summing_table;
