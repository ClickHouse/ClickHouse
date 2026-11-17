SELECT netloc('http://user:pass@[2001:db8::1]:8080/path');
SELECT netloc('http://[2001:db8::1]:8080/path');
SELECT netloc('http://user:pass@[::ffff:192.168.0.1]:8080/');
SELECT netloc('http://user:pass@example.com:8080/path');
SELECT netloc('https://example.com/');
SELECT netloc('http://user@example.com?x=1');
