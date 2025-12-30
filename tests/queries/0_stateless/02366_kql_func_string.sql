-- Tags: no-fasttest

DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28), ('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);

-- datatable (Version:string) [
--     '1.2.3.4',
--     '1.2',
--     '1.2.3',
--     '1'
-- ]

DROP TABLE IF EXISTS Versions;
CREATE TABLE Versions
(    
    Version String
) ENGINE = Memory;
INSERT INTO Versions VALUES ('1.2.3.4'),('1.2'),('1.2.3'),('1');


set allow_experimental_kusto_dialect=1;
set dialect='kusto';
print '-- test String Functions --';

print '-- Customers |where Education contains \'degree\'';
Customers |where Education contains 'degree' | order by LastName;
print '';
print '-- Customers |where Education !contains \'degree\'';
Customers |where Education !contains 'degree' | order by LastName;
print '';
print '-- Customers |where Education contains \'Degree\'';
Customers |where Education contains 'Degree' | order by LastName;
print '';
print '-- Customers |where Education !contains \'Degree\'';
Customers |where Education !contains 'Degree' | order by LastName;
print '';
print '-- Customers | where FirstName endswith \'RE\'';
Customers | where FirstName endswith 'RE' | order by LastName;
print '';
print '-- Customers | where ! FirstName endswith \'RE\'';
Customers | where FirstName ! endswith 'RE' | order by LastName;
print '';
print '--Customers | where FirstName endswith_cs \'re\'';
Customers | where FirstName endswith_cs 're' | order by LastName;
print '';
print '-- Customers | where FirstName !endswith_cs \'re\'';
Customers | where FirstName !endswith_cs 're' | order by LastName;
print '';
print '-- Customers | where Occupation == \'Skilled Manual\'';
Customers | where Occupation == 'Skilled Manual' | order by LastName;
print '';
print '-- Customers | where Occupation != \'Skilled Manual\'';
Customers | where Occupation != 'Skilled Manual' | order by LastName;
print '';
print '-- Customers | where Occupation has \'skilled\'';
Customers | where Occupation has 'skilled' | order by LastName;
print '';
print '-- Customers | where Occupation !has \'skilled\'';
Customers | where Occupation !has 'skilled' | order by LastName;
print '';
print '-- Customers | where Occupation has \'Skilled\'';
Customers | where Occupation has 'Skilled'| order by LastName;
print '';
print '-- Customers | where Occupation !has \'Skilled\'';
Customers | where Occupation !has 'Skilled'| order by LastName;
print '';
print '-- Customers | where Occupation hasprefix_cs \'Ab\'';
Customers | where Occupation hasprefix_cs 'Ab'| order by LastName;
print '';
print '-- Customers | where Occupation !hasprefix_cs \'Ab\'';
Customers | where Occupation !hasprefix_cs 'Ab'| order by LastName;
print '';
print '-- Customers | where Occupation hasprefix_cs \'ab\'';
Customers | where Occupation hasprefix_cs 'ab'| order by LastName;
print '';
print '-- Customers | where Occupation !hasprefix_cs \'ab\'';
Customers | where Occupation !hasprefix_cs 'ab'| order by LastName;
print '';
print '-- Customers | where Occupation hassuffix \'Ent\'';
Customers | where Occupation hassuffix 'Ent'| order by LastName;
print '';
print '-- Customers | where Occupation !hassuffix \'Ent\'';
Customers | where Occupation !hassuffix 'Ent'| order by LastName;
print '';
print '-- Customers | where Occupation hassuffix \'ent\'';
Customers | where Occupation hassuffix 'ent'| order by LastName;
print '';
print '-- Customers | where Occupation hassuffix \'ent\'';
Customers | where Occupation hassuffix 'ent'| order by LastName;
print '';
print '-- Customers |where Education in (\'Bachelors\',\'High School\')';
Customers |where Education in ('Bachelors','High School')| order by LastName;
print '';
print '-- Customers | where Education !in (\'Bachelors\',\'High School\')';
Customers | where Education !in ('Bachelors','High School')| order by LastName;
print '';
print '-- Customers | where FirstName matches regex \'P.*r\'';
Customers | where FirstName matches regex 'P.*r'| order by LastName;
print '';
print '-- Customers | where FirstName startswith \'pet\'';
Customers | where FirstName startswith 'pet'| order by LastName;
print '';
print '-- Customers | where FirstName !startswith \'pet\'';
Customers | where FirstName !startswith 'pet'| order by LastName;
print '';
print '-- Customers | where FirstName startswith_cs \'pet\'';
Customers | where FirstName startswith_cs 'pet'| order by LastName;
print '';
print '-- Customers | where FirstName !startswith_cs \'pet\'';
Customers | where FirstName !startswith_cs 'pet'| order by LastName;
print '';
print '-- Customers | where isempty(LastName)';
Customers | where isempty(LastName);
print '';
print '-- Customers | where isnotempty(LastName)';
Customers | where isnotempty(LastName);
print '';
print '-- Customers | where isnotnull(FirstName)';
Customers | where isnotnull(FirstName)| order by LastName;
print '';
print '-- Customers | where isnull(FirstName)';
Customers | where isnull(FirstName)| order by LastName;
print '';
print '-- Customers | project url_decode(\'https%3A%2F%2Fwww.test.com%2Fhello%20word\') | take 1';
Customers | project url_decode('https%3A%2F%2Fwww.test.com%2Fhello%20word') | take 1;
print '';
print '-- Customers | project url_encode(\'https://www.test.com/hello word\') | take 1';
Customers | project url_encode('https://www.test.com/hello word') | take 1;
print '';
print '-- Customers | project name_abbr = strcat(substring(FirstName,0,3), \' \', substring(LastName,2))';
Customers | project name_abbr = strcat(substring(FirstName,0,3), ' ', substring(LastName,2))| order by LastName;
print '';
print '-- Customers | project name = strcat(FirstName, \' \', LastName)';
Customers | project name = strcat(FirstName, ' ', LastName)| order by LastName;
print '';
print '-- Customers | project FirstName, strlen(FirstName)';
Customers | project FirstName, strlen(FirstName)| order by LastName;
print '';
print '-- Customers | project strrep(FirstName,2,\'_\')';
Customers | project strrep(FirstName,2,'_')| order by LastName;
print '';
print '-- Customers | project toupper(FirstName)';
Customers | project toupper(FirstName)| order by LastName;
print '';
print '-- Customers | project tolower(FirstName)';
Customers | project tolower(FirstName)| order by LastName;
print '';
print '-- support subquery for in orerator (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/in-cs-operator) (subquery need to be wraped with bracket inside bracket); TODO: case-insensitive not supported yet';
Customers | where Age in ((Customers|project Age|where Age < 30)) | order by LastName;
-- Customer | where LastName in~ ("diaz", "cox")
print '';
print '-- has_all (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-all-operator); TODO: subquery not supported yet';
Customers | where Occupation has_all ('manual', 'skilled') | order by LastName;
print '';
print '-- has_any (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-anyoperator); TODO: subquery not supported yet';
Customers|where Occupation has_any ('Skilled','abcd');
print '';
print '-- countof (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countoffunction)';
Customers | project countof('The cat sat on the mat', 'at') | take 1;
Customers | project countof('The cat sat on the mat', 'at', 'normal') | take 1;
Customers | project countof('The cat sat on the mat', '\\s.he', 'regex') | take 1;
print '';
print '-- extract ( https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractfunction)';
print extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 0, 'The price of PINEAPPLE ice cream is 20');
print extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 1, 'The price of PINEAPPLE ice cream is 20');
print extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 20');
print extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 3, 'The price of PINEAPPLE ice cream is 20');
print extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 20', typeof(real));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(bool));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(date));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(guid));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(int));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(long));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(real));
print extract("x=([0-9.]+)", 1, "hello x=45.6|wo" , typeof(decimal));
print '';
print '-- extract_all (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractallfunction); TODO: captureGroups not supported yet';
Customers | project extract_all('(\\w)(\\w+)(\\w)','The price of PINEAPPLE ice cream is 20') | take 1;
print '';
print '-- extract_json (https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/extractjsonfunction)';
print extract_json('', ''); -- { serverError BAD_ARGUMENTS }
print extract_json('a', ''); -- { serverError BAD_ARGUMENTS }
print extract_json('$.firstName', '');
print extract_json('$.phoneNumbers[0].type', '');
print extractjson('$.firstName', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}');
print extract_json('$.phoneNumbers[0].type', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(string));
print extract_json('$.phoneNumbers[0].type', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(int));
print extract_json('$.age', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}');
print extract_json('$.age', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(int));
print extract_json('$.age', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(long));
-- print extract_json('$.age', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(bool)); -> true
print extract_json('$.age', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(double));
print extract_json('$.age', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(guid));
-- print extract_json('$.phoneNumbers', '{"firstName":"John","lastName":"doe","age":26,"address":{"streetAddress":"naist street","city":"Nara","postalCode":"630-0192"},"phoneNumbers":[{"type":"iPhone","number":"0123-4567-8888"},{"type":"home","number":"0123-4567-8910"}]}', typeof(dynamic)); we won't be able to handle this particular case for a while, because it should return a dictionary
print '';
print '-- split (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction)';
Customers | project split('aa_bb', '_') | take 1;
Customers | project split('aaa_bbb_ccc', '_', 1) | take 1;
Customers | project split('', '_') | take 1;
Customers | project split('a__b', '_') | take 1;
Customers | project split('aabbcc', 'bb') | take 1;
Customers | project split('aabbcc', '') | take 1;
Customers | project split('aaa_bbb_ccc', '_', -1) | take 1;
Customers | project split('aaa_bbb_ccc', '_', 10) | take 1;
print '';
print '-- strcat_delim (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-delimfunction); TODO: only support string now.';
Customers | project strcat_delim('-', '1', '2', strcat('A','b')) | take 1;
-- Customers | project strcat_delim('-', '1', '2', 'A' , 1s);
print '';
print '-- indexof (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/indexoffunction); TODO: length and occurrence not supported yet';
Customers | project indexof('abcdefg','cde') | take 1;
Customers | project indexof('abcdefg','cde',2) | take 1;
Customers | project indexof('abcdefg','cde',6) | take 1;
print '-- base64_encode_fromguid()';
-- print base64_encode_fromguid(guid(null));
print base64_encode_fromguid(guid('ae3133f2-6e22-49ae-b06a-16e6a9b212eb'));
print base64_encode_fromguid(dynamic(null)); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print base64_encode_fromguid("abcd1231"); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print '-- base64_decode_toarray()';
print base64_decode_toarray('');
print base64_decode_toarray('S3VzdG8=');
print '-- base64_decode_toguid()';
print base64_decode_toguid("JpbpECu8dUy7Pv5gbeJXAA==");
print base64_decode_toguid(base64_encode_fromguid(guid('ae3133f2-6e22-49ae-b06a-16e6a9b212eb'))) == guid('ae3133f2-6e22-49ae-b06a-16e6a9b212eb');
print '-- base64_encode_tostring';
print base64_encode_tostring('');
print base64_encode_tostring('Kusto1');
print '-- base64_decode_tostring';
print base64_decode_tostring('');
print base64_decode_tostring('S3VzdG8x');
print '-- parse_url()';
print parse_url('scheme://username:password@host:1234/this/is/a/path?k1=v1&k2=v2#fragment');
print '-- parse_urlquery()';
print parse_urlquery('k1=v1&k2=v2&k3=v3');
print '-- strcmp()';
print strcmp('ABC','ABC'), strcmp('abc','ABC'), strcmp('ABC','abc'), strcmp('abcde','abc');
print '-- substring()';
print substring("ABCD", -2, 2);
print '-- translate()';
print translate('krasp', 'otsku', 'spark'), translate('abc', '', 'ab'), translate('abc', 'x', 'abc');
print '-- trim()';
print trim("--", "--https://www.ibm.com--");
print trim("[^\w]+", strcat("- ","Te st", "1", "// $"));
print trim("", " asd ");
print trim("a$", "asd");
print trim("^a", "asd");
print '-- trim_start()';
print trim_start("https://", "https://www.ibm.com");
print trim_start("[^\w]+", strcat("-  ","Te st", "1", "// $"));
print trim_start("asd$", "asdw");
print trim_start("asd$", "asd");
print trim_start("d$", "asd");
print '-- trim_end()';
print trim_end("://www.ibm.com", "https://www.ibm.com");
print trim_end("[^\w]+", strcat("- ","Te st", "1", "// $"));
print trim_end("^asd", "wasd");
print trim_end("^asd", "asd");
print trim_end("^a", "asd");
print '-- trim, trim_start, trim_end all at once';
print str = "--https://bing.com--", pattern = '--' | extend start = trim_start(pattern, str), end = trim_end(pattern, str), both = trim(pattern, str);
print '-- replace_regex';
print replace_regex(strcat('Number is ', '1'), 'is (\d+)', 'was: \1');
print '-- has_any_index()';
print has_any_index('this is an example', dynamic(['this', 'example'])), has_any_index("this is an example", dynamic(['not', 'example'])), has_any_index("this is an example", dynamic(['not', 'found'])), has_any_index("this is an example", dynamic([]));
print '-- parse_version()';
print parse_version(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- print parse_version(''); -> NULL
print parse_version('1.2.3.40');
print parse_version('1.2');
print parse_version(strcat('1.', '2'));
print parse_version('1.2.4.5.6');
print parse_version('moo'); 
print parse_version('moo.boo.foo');
print parse_version(strcat_delim('.', 'moo', 'boo', 'foo'));
Versions | project parse_version(Version);
print '-- parse_json()';
print parse_json(dynamic([1, 2, 3]));
print parse_json('{"a":123.5, "b":"{\\"c\\":456}"}');
print '-- parse_command_line()';
print parse_command_line(55, 'windows'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- print parse_command_line((52 + 3) * 4 % 2, 'windows'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print parse_command_line('', 'windows');
print parse_command_line(strrep(' ', 6), 'windows'); 
-- print parse_command_line('echo \"hello world!\" print$?', 'windows'); -> ["echo","hello world!","print$?"]
-- print parse_command_line("yolo swag 'asd bcd' \"moo moo \"", 'windows'); -> ["yolo","swag","'asd","bcd'","moo moo "]
-- print parse_command_line(strcat_delim(' ', "yolo", "swag", "\'asd bcd\'", "\"moo moo \""), 'windows'); -> ["yolo","swag","'asd","bcd'","moo moo "]
print '-- reverse()';
print reverse(123);
print reverse(123.34);
print reverse('');
print reverse("asd");
print reverse(dynamic([]));
print reverse(dynamic([1, 2, 3]));
print reverse(dynamic(['Darth', "Vader"]));
print reverse(datetime(2017-10-15 12:00));
-- print reverse(timespan(3h)); -> 00:00:30
Customers | where Education contains 'degree' | order by reverse(FirstName);
print '-- parse_csv()';
print parse_csv('');
print parse_csv(65); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
print parse_csv('aaa');
print result=parse_csv('aa,b,cc');
print result_multi_record=parse_csv('record1,a,b,c\nrecord2,x,y,z');
-- print result=parse_csv('aa,"b,b,b",cc,"Escaping quotes: ""Title""","line1\nline2"'); -> ["aa","b,b,b","cc","Escaping quotes: \"Title\"","line1\nline2"]
-- print parse_csv(strcat(strcat_delim(',', 'aa', '"b,b,b"', 'cc', '"Escaping quotes: ""Title"""', '"line1\nline2"'), '\r\n', strcat_delim(',', 'asd', 'qcf'))); -> ["aa","b,b,b","cc","Escaping quotes: \"Title\"","line1\nline2"]
