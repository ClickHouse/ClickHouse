-- { echoOn }
SELECT NOT 0 + NOT 0;
SELECT NOT (0 + (NOT 0));
SELECT (NOT 0) + (NOT 0);
SELECT formatQuery('SELECT NOT 0 + NOT 0');
SELECT formatQuery('SELECT NOT (0 + (NOT 0))');
SELECT formatQuery('SELECT (NOT 0) + (NOT 0)');
