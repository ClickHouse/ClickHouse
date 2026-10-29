-- Every spelling of the IPv6 unspecified address is an address. The peer `::6` of a proxy is
-- serialized as `::`, and a secondary query is rejected if `::` is not parsed back.
DROP USER IF EXISTS u_05385_a, u_05385_b, u_05385_c;

CREATE USER u_05385_a HOST IP '::';
CREATE USER u_05385_b HOST IP '0:0:0:0:0:0:0:0';
CREATE USER u_05385_c HOST IP '::0.0.0.0';

SHOW CREATE USER u_05385_a;
SHOW CREATE USER u_05385_b;
SHOW CREATE USER u_05385_c;

DROP USER u_05385_a, u_05385_b, u_05385_c;
