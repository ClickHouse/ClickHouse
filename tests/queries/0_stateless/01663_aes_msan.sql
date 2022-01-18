SELECT hex(encrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', ''));
SELECT decrypt('aes-128-cbc', encrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', ''), 'keykeykeykeykeyk', '');
