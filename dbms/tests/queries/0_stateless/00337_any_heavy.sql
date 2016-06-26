SELECT anyHeavy(x) FROM (SELECT intHash64(number) % 100 < 60 ? 999 : number AS x FROM system.numbers LIMIT 100000);
