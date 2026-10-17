-- `Poco::Net::IPAddress::toString` used to drop the dotted part of `::0.x.y.z` and `::ffff:0.x.y.z`,
-- so the addresses below were shown as `::`, `::ffff:`, `::ffff:` and `::1.2.3.4`.
DROP USER IF EXISTS test_user_05385;
CREATE USER test_user_05385 HOST IP '::0.1.2.3', '::ffff:0.0.0.6', '::ffff:0.0.0.0', '::1.2.3.4';
SHOW CREATE USER test_user_05385;
DROP USER test_user_05385;
