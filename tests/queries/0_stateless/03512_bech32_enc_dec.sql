-- baseline test, encode of value should match expected val
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- different hrp value should yield a different result
select bech32Encode('tb', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- test with explicit witver = 0, should be same as default
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0) == 
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- testing bech32m algo
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1);
select bech32Encode('tb', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 2);

-- witversions >=1 should all be the same
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1) == 
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 2);

-- decode valid string, witver 0, hrp=bc
select bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z');
-- decode valid string, witver 1, hrp=tb
select bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v');
-- decode valid string, witver 1, hrp=bc, should be same as witver 0 since same string was used to encode them
select bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7k8zcwmq') ==
       bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z');
-- decode valid string, witver 1, hrp=tb, see above comment
select bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7khalasw') ==
       bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v');


