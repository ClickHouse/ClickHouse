drop table if exists geohash_test_data;

create table geohash_test_data (
	latitude  Float64,
	longitude Float64,
	encoded   String
) engine = MergeTree order by (latitude, longitude, encoded);

-- data obtained from geohash.com
insert into geohash_test_data values (-25.427, -49.315, '6'), (-25.427, -49.315, '6g'), (-25.427, -49.315, '6gk'), (-25.427, -49.315, '6gkz'), (-25.427, -49.315, '6gkzm'), (-25.427, -49.315, '6gkzmg'), (-25.427, -49.315, '6gkzmg1'), (-25.427, -49.315, '6gkzmg1u'), (-25.383, -49.266, '6'), (-25.383, -49.266, '6g'), (-25.383, -49.266, '6gk'), (-25.383, -49.266, '6gkz'), (-25.383, -49.266, '6gkzw'), (-25.383, -49.266, '6gkzwg'), (-25.383, -49.266, '6gkzwgj'), (-25.383, -49.266, '6gkzwgjt'), (-25.382708, -49.265506, '6'), (-25.382708, -49.265506, '6g'), (-25.382708, -49.265506, '6gk'), (-25.382708, -49.265506, '6gkz'), (-25.382708, -49.265506, '6gkzw'), (-25.382708, -49.265506, '6gkzwg'), (-25.382708, -49.265506, '6gkzwgj'), (-25.382708, -49.265506, '6gkzwgjz'), (-25.382708, -49.265506, '6gkzwgjzn'), (-25.382708, -49.265506, '6gkzwgjzn8'), (-25.382708, -49.265506, '6gkzwgjzn82'), (-25.382708, -49.265506, '6gkzwgjzn820'), (-0.1, -0.1, '7'), (-0.1, -0.1, '7z'), (-0.1, -0.1, '7zz'), (-0.1, -0.1, '7zzz'), (-0.1, -0.1, '7zzzm'), (-0.1, -0.01, '7'), (-0.1, -0.01, '7z'), (-0.1, -0.01, '7zz'), (-0.1, -0.01, '7zzz'), (-0.1, -0.01, '7zzzr'), (-0.1, -0.01, '7zzzrv'), (-0.1, -0.01, '7zzzrvb'), (-0.1, 0, 'k'), (-0.1, 0, 'kp'), (-0.1, 0, 'kpb'), (-0.1, 0, 'kpbp'), (-0.1, 0, 'kpbp2'), (-0.1, 0.01, 'k'), (-0.1, 0.01, 'kp'), (-0.1, 0.01, 'kpb'), (-0.1, 0.01, 'kpbp'), (-0.1, 0.01, 'kpbp2'), (-0.1, 0.01, 'kpbp2j'), (-0.1, 0.01, 'kpbp2jz'), (-0.1, 0.1, 'k'), (-0.1, 0.1, 'kp'), (-0.1, 0.1, 'kpb'), (-0.1, 0.1, 'kpbp'), (-0.1, 0.1, 'kpbp6'), (-0.01, -0.1, '7'), (-0.01, -0.1, '7z'), (-0.01, -0.1, '7zz'), (-0.01, -0.1, '7zzz'), (-0.01, -0.1, '7zzzv'), (-0.01, -0.1, '7zzzvw'), (-0.01, -0.01, '7'), (-0.01, -0.01, '7z'), (-0.01, -0.01, '7zz'), (-0.01, -0.01, '7zzz'), (-0.01, -0.01, '7zzzz'), (-0.01, -0.01, '7zzzzy'), (-0.01, -0.01, '7zzzzy0'), (-0.01, 0, 'k'), (-0.01, 0, 'kp'), (-0.01, 0, 'kpb'), (-0.01, 0, 'kpbp'), (-0.01, 0, 'kpbpb'), (-0.01, 0, 'kpbpbn'), (-0.01, 0.01, 'k'), (-0.01, 0.01, 'kp'), (-0.01, 0.01, 'kpb'), (-0.01, 0.01, 'kpbp'), (-0.01, 0.01, 'kpbpb'), (-0.01, 0.01, 'kpbpbn'), (-0.01, 0.01, 'kpbpbnp'), (-0.01, 0.1, 'k'), (-0.01, 0.1, 'kp'), (-0.01, 0.1, 'kpb'), (-0.01, 0.1, 'kpbp'), (-0.01, 0.1, 'kpbpf'), (-0.01, 0.1, 'kpbpfq'), (0, -0.1, 'e'), (0, -0.1, 'eb'), (0, -0.1, 'ebp'), (0, -0.1, 'ebpb'), (0, -0.1, 'ebpbj'), (0, -0.01, 'e'), (0, -0.01, 'eb'), (0, -0.01, 'ebp'), (0, -0.01, 'ebpb'), (0, -0.01, 'ebpbp'), (0, -0.01, 'ebpbpb'), (0, -0.01, 'ebpbpb0'), (0, 0, 's'), (0, 0, 's0'), (0, 0, 's00'), (0, 0, 's000'), (0, 0.01, 's'), (0, 0.01, 's0'), (0, 0.01, 's00'), (0, 0.01, 's000'), (0, 0.01, 's0000'), (0, 0.01, 's00000'), (0, 0.01, 's00000p'), (0, 0.1, 's'), (0, 0.1, 's0'), (0, 0.1, 's00'), (0, 0.1, 's000'), (0, 0.1, 's0004'), (0.01, -0.1, 'e'), (0.01, -0.1, 'eb'), (0.01, -0.1, 'ebp'), (0.01, -0.1, 'ebpb'), (0.01, -0.1, 'ebpbj'), (0.01, -0.1, 'ebpbj9'), (0.01, -0.01, 'e'), (0.01, -0.01, 'eb'), (0.01, -0.01, 'ebp'), (0.01, -0.01, 'ebpb'), (0.01, -0.01, 'ebpbp'), (0.01, -0.01, 'ebpbpc'), (0.01, -0.01, 'ebpbpcb'), (0.01, 0, 's'), (0.01, 0, 's0'), (0.01, 0, 's00'), (0.01, 0, 's000'), (0.01, 0, 's0000'), (0.01, 0, 's00001'), (0.01, 0.01, 's'), (0.01, 0.01, 's0'), (0.01, 0.01, 's00'), (0.01, 0.01, 's000'), (0.01, 0.01, 's0000'), (0.01, 0.01, 's00001'), (0.01, 0.01, 's00001z'), (0.01, 0.1, 's'), (0.01, 0.1, 's0'), (0.01, 0.1, 's00'), (0.01, 0.1, 's000'), (0.01, 0.1, 's0004'), (0.01, 0.1, 's00043'), (0.1, -0.1, 'e'), (0.1, -0.1, 'eb'), (0.1, -0.1, 'ebp'), (0.1, -0.1, 'ebpb'), (0.1, -0.1, 'ebpbt'), (0.1, -0.01, 'e'), (0.1, -0.01, 'eb'), (0.1, -0.01, 'ebp'), (0.1, -0.01, 'ebpb'), (0.1, -0.01, 'ebpbx'), (0.1, -0.01, 'ebpbxf'), (0.1, -0.01, 'ebpbxf0'), (0.1, 0, 's'), (0.1, 0, 's0'), (0.1, 0, 's00'), (0.1, 0, 's000'), (0.1, 0, 's0008'), (0.1, 0.01, 's'), (0.1, 0.01, 's0'), (0.1, 0.01, 's00'), (0.1, 0.01, 's000'), (0.1, 0.01, 's0008'), (0.1, 0.01, 's00084'), (0.1, 0.01, 's00084p'), (0.1, 0.1, 's'), (0.1, 0.1, 's0'), (0.1, 0.1, 's00'), (0.1, 0.1, 's000'), (0.1, 0.1, 's000d'), (7.880886, 98.3640363, 'w'), (7.880886, 98.3640363, 'w1'), (7.880886, 98.3640363, 'w1m'), (7.880886, 98.3640363, 'w1mu'), (7.880886, 98.3640363, 'w1muy'), (7.880886, 98.3640363, 'w1muy6'), (7.880886, 98.3640363, 'w1muy6d'), (7.880886, 98.3640363, 'w1muy6dt'), (7.880886, 98.3640363, 'w1muy6dt2'), (7.880886, 98.3640363, 'w1muy6dt2p'), (7.880886, 98.3640363, 'w1muy6dt2pt'), (7.880886, 98.3640363, 'w1muy6dt2ptk'), (51.523242, -0.07914, 'g'), (51.523242, -0.07914, 'gc'), (51.523242, -0.07914, 'gcp'), (51.523242, -0.07914, 'gcpv'), (51.523242, -0.07914, 'gcpvn'), (51.523242, -0.07914, 'gcpvn5'), (51.523242, -0.07914, 'gcpvn5w'), (51.523242, -0.07914, 'gcpvn5w2'), (51.523242, -0.07914, 'gcpvn5w2e'), (51.523242, -0.07914, 'gcpvn5w2eu'), (51.523242, -0.07914, 'gcpvn5w2euk'), (51.523242, -0.07914, 'gcpvn5w2euky'), (53.923107, 27.606682, 'u'), (53.923107, 27.606682, 'u9'), (53.923107, 27.606682, 'u9e'), (53.923107, 27.606682, 'u9ed'), (53.923107, 27.606682, 'u9edu'), (53.923107, 27.606682, 'u9edu0'), (53.923107, 27.606682, 'u9edu0q'), (53.923107, 27.606682, 'u9edu0qs'), (53.923107, 27.606682, 'u9edu0qsf'), (53.923107, 27.606682, 'u9edu0qsf7'), (53.923107, 27.606682, 'u9edu0qsf7d'), (53.923107, 27.606682, 'u9edu0qsf7dn');


select 'invalid values:'; -- must not crash
select geohashEncode(181.0, 91.0);
select geohashEncode(-181.0, -91.0);
select count(geohashDecode('abcdefghijklmnopqrstuvwxyz'));

select 'constant values:';
select geohashEncode(-5.60302734375, 42.593994140625, 0);
select round(geohashDecode('ezs42').1, 5), round(geohashDecode('ezs42').2, 5);

select 'default precision:';
select geohashEncode(-5.60302734375, 42.593994140625);

select 'mixing const and non-const-columns:';
select geohashEncode(materialize(-5.60302734375), materialize(42.593994140625), 0);
select geohashEncode(materialize(-5.60302734375), materialize(42.593994140625), materialize(0)); -- { serverError 44 }


select 'from table (with const precision):';

-- here results are strings, so reference may contain values to match for equality.
select 1 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 2 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 3 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 4 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 5 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 6 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 7 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 8 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 9 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 10 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 11 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;
select 12 as p, geohashEncode(longitude, latitude, p) as actual, if(actual = encoded, 'Ok', concat('expected: ', encoded)) from geohash_test_data WHERE length(encoded) = p;

-- Here results are floats, and hence may not be compared for equality directly.
-- We select all values that are off by some reasonable value:
-- each byte of encoded string provides 5 bits of precision, (roughly 2.5 for lon and lat)
-- each bit of precision divides value range by 2.
-- hence max error is roughly value range 2.5 times divided by 2 for each precision bit.
-- initial value range is [-90..90] for latitude and [-180..180] for longitude.
select 'incorrectly decoded values:';
select
	geohashDecode(encoded) as actual,
	'expected:', encoded, '=>', latitude, longitude,
	'length:', 	length(encoded),
	'max lat error:', 180 / power(2, 2.5 * length(encoded)) as latitude_max_error,
	'max lon error:', 360 / power(2, 2.5 * length(encoded)) as longitude_max_error,
	'err:', (actual.2 - latitude) as lat_error, (actual.1 - longitude) as lon_error,
	'derr:', abs(lat_error) - latitude_max_error, abs(lon_error) - longitude_max_error
from geohash_test_data
where
	abs(lat_error) > latitude_max_error
	or
	abs(lon_error) > longitude_max_error;

drop table if exists geohash_test_data;
