WITH (SELECT bitmapBuild(groupArray(number)) FROM (SELECT number FROM system.numbers limit 10) t) as z SELECT bitmapContains(z, number) from system.numbers limit 20
