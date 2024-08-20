-- Ouput can be verified using: https://emn178.github.io/online-tools/ripemd-160/

SELECT hex(ripeMD160('The quick brown fox jumps over the lazy dog'));

SELECT hex(ripeMD160('The quick brown fox jumps over the lazy cog'));

SELECT hex(ripeMD160(''));

SELECT hex(ripeMD160('CheREpaha1512'));

SELECT hex(ripeMD160('A-very-long-string-that-should-be-hashed-using-ripeMD160'));
