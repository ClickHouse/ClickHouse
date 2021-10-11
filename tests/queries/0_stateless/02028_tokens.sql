SELECT tokens('test');
SELECT tokens('test1, test2, test3');
SELECT tokens('test1, test2,     test3, test4');
SELECT tokens('test1,;\ test2,;\ test3,;\   test4');

SELECT tokens(materialize('test'));
SELECT tokens(materialize('test1, test2, test3'));
SELECT tokens(materialize('test1, test2,     test3, test4'));
SELECT tokens(materialize('test1,;\ test2,;\ test3,;\   test4'));
