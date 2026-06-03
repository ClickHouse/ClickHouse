SELECT hasSubsequenceUTF8('', '');
SELECT hasSubsequenceUTF8('', 'a');
SELECT hasSubsequenceCaseInsensitiveUTF8('', '');
SELECT hasSubsequenceCaseInsensitiveUTF8('', 'A');

SELECT hasSubsequenceUTF8(materialize(''), materialize(''));
SELECT hasSubsequenceUTF8(materialize(''), materialize('a'));
SELECT hasSubsequenceCaseInsensitiveUTF8(materialize(''), materialize(''));
SELECT hasSubsequenceCaseInsensitiveUTF8(materialize(''), materialize('A'));
