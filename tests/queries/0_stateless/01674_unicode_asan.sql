SELECT positionCaseInsensitiveUTF8('иголка.ру', 'иголка.р�\0') AS res;
SELECT positionCaseInsensitiveUTF8('иголка.ру', randomString(rand() % 100)) FROM system.numbers; -- { serverError 2 }
