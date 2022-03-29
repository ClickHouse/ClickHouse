DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(ip String, ipv6 IPv6 MATERIALIZED toIPv6(ip)) ENGINE = TinyLog;

INSERT INTO test_table(ip) VALUES ('fe80::9801:43ff:fe1f:7690'), ('1.1.1.1'), (''), ('::ffff:1.1.1.1' ); --{serverError 441}

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

INSERT INTO test_table(ip) VALUES ( 'fe80::9801:43ff:fe1f:7690'), ('1.1.1.1'), (''), ('::ffff:1.1.1.1' );
SELECT * FROM test_table;

DROP TABLE test_table;
