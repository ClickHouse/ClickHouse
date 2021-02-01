SELECT lcm(15, 10);
SELECT lcm(-15, 10);
SELECT lcm(15, -10);
SELECT lcm(-15, -10);

-- Implementation specific result on overflow:
SELECT ignore(lcm(256, 9223372036854775807));
SELECT ignore(lcm(256, -9223372036854775807));
SELECT ignore(lcm(-256, 9223372036854775807));
SELECT ignore(lcm(-256, -9223372036854775807));
