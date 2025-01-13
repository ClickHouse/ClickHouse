CREATE DICTIONARY testip
(
    `network` String, 
    `test_field` String
)
PRIMARY KEY network
SOURCE(FILE(PATH '/tmp/test.csv' FORMAT CSVWithNames))
LIFETIME(MIN 0 MAX 300)
LAYOUT(IPTRIE()); -- { serverError 137 }
