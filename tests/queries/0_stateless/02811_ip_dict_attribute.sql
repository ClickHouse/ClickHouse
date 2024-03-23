CREATE TABLE src ( id UInt64, ip4 IPv4, ip6 IPv6 ) Engine=Memory AS
  SELECT * FROM VALUES( (1, '1.1.1.1', '::1.1.1.1'), (2, '2.2.2.2', '::2.2.2.2') );

CREATE DICTIONARY dict ( id UInt64, ip4 IPv4, ip6 IPv6 )
  PRIMARY KEY id
  LAYOUT(HASHED())
  SOURCE (CLICKHOUSE ( table src))
  lifetime ( 10);

SELECT dictGet('dict', ('ip6', 'ip4'), arrayJoin([2,1]));

DROP DICTIONARY dict;
DROP TABLE src;
