-- test gcd
select gcd(1280, 1024);
select gcd(11, 121);
select gcd(-256, 64);
select gcd(1, 1);
select gcd(4, 2);
select gcd(15, 49);
select gcd(255, 254);
select gcd(2147483647, 2147483646);
select gcd(4611686011984936962, 2147483647);
select gcd(-2147483648, 1);
select gcd(255, 515);
select gcd(255, 510);
select gcd(255, 512);
-- test lcm
select lcm(1280, 1024);
select lcm(11, 121);
select lcm(-256, 64);
select lcm(1, 1);
select lcm(4, 2);
select lcm(15, 49);
select lcm(255, 254);
select lcm(2147483647, 2147483646);
select lcm(4611686011984936962, 2147483647);
select lcm(-2147483648, 1);
-- test gcd float
select gcd(1280.1, 1024.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select gcd(11.1, 121.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select gcd(-256.1, 64.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select gcd(1.1, 1.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select gcd(4.1, 2.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select gcd(15.1, 49.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select gcd(255.1, 254.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- test lcm float
select lcm(1280.1, 1024.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select lcm(11.1, 121.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select lcm(-256.1, 64.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select lcm(1.1, 1.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select lcm(4.1, 2.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select lcm(15.1, 49.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select lcm(255.1, 254.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
