-- MySQL has function `unhex`, so we will make our function `unhex` also case insensitive for compatibility.
SELECT unhex('303132'), UNHEX('4D7953514C');
