select visitParamExtractInt('{"myparam":-1}', 'myparam') AS res;
select visitParamExtractUInt('{"myparam":-1}', 'myparam') AS res;
select visitParamExtractFloat('{"myparam":null}', 'myparam') AS res;
select visitParamExtractFloat('{"myparam":-1}', 'myparam') AS res;
