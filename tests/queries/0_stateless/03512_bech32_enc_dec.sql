-- baseline test, encode of value should match expected val
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- different hrp value should yield a different result
select bech32Encode('tb', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- exactly the max amount of characters (50) should work
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d45494'));

-- test other hrps
select bech32Encode('bcrt', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
select bech32Encode('tltc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
select bech32Encode('tltssdfsdvjnasdfnjkbhksdfasnbdfkljhaksdjfnakjsdhasdfnasdkfasdfasdfasdf', unhex('751e'));
-- too many chars
select bech32Encode('tltssdfsdvjnasdfnjkbhksdfasnbdfkljhaksdjfnakjsdhasdfnasdkfasdfasdfasdfdljsdfasdfahc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- negative tests
-- empty hrp
select bech32Encode('', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- 51 chars should return nothing
select bech32Encode('', unhex('751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d45494a'));

-- test with explicit witver = 0, should be same as default
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0) == 
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- testing bech32m algo
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1);
select bech32Encode('tb', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 2);

-- witversions >=1 should all be the same
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1) == 
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 10);

select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'))) as tup);

-- negative tests
select bech32Decode('');
select bech32Decode('foo');
select bech32Decode('751E76E8199196D454941C45D1B3A323F1433BD6');

-- decode valid string, witver 0, hrp=bc
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z') as tup);
-- decode valid string, witver 1, hrp=tb
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v') as tup);
-- decode valid string, witver 1, hrp=bc, should be same as witver 0 since same string was used to encode them
select bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7k8zcwmq') ==
       bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z');
-- decode valid string, witver 1, hrp=tb, see above comment
select bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7khalasw') ==
       bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v');

-- testing max length, this should work
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('b1w508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5xgqsaanm') as tup);
-- testing max length, this should returun nothing
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('b1w508dfqejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5xgqsaanm') as tup);

-- TODO test fixed strings

