-- { echo }

SELECT materialize([[13]])[1::Int8];
SELECT materialize([['Hello']])[1::Int8];
SELECT materialize([13])[1::Int8];
SELECT materialize(['Hello'])[1::Int8];

SELECT materialize([[13], [14]])[2::Int8];
SELECT materialize([['Hello'], ['world']])[2::Int8];
SELECT materialize([13, 14])[2::Int8];
SELECT materialize(['Hello', 'world'])[2::Int8];

SELECT materialize([[13], [14]])[3::Int8];
SELECT materialize([['Hello'], ['world']])[3::Int8];
SELECT materialize([13, 14])[3::Int8];
SELECT materialize(['Hello', 'world'])[3::Int8];

SELECT materialize([[13], [14]])[0::Int8];
SELECT materialize([['Hello'], ['world']])[0::Int8];
SELECT materialize([13, 14])[0::Int8];
SELECT materialize(['Hello', 'world'])[0::Int8];

SELECT materialize([[13], [14]])[-1];
SELECT materialize([['Hello'], ['world']])[-1];
SELECT materialize([13, 14])[-1];
SELECT materialize(['Hello', 'world'])[-1];

SELECT materialize([[13], [14]])[-9223372036854775808];
SELECT materialize([['Hello'], ['world']])[-9223372036854775808];
SELECT materialize([13, 14])[-9223372036854775808];
SELECT materialize(['Hello', 'world'])[-9223372036854775808];

SELECT materialize([[toNullable(13)], [14]])[-9223372036854775808];
SELECT materialize([['Hello'], [toNullable('world')]])[-9223372036854775808];
SELECT materialize([13, toNullable(14)])[-9223372036854775808];
SELECT materialize(['Hello', toLowCardinality('world')])[-9223372036854775808];
