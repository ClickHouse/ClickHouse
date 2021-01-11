SELECT * FROM url('http://127.0.0.1:1337/? HTTP/1.1\r\nTest: test', CSV, 'column1 String'); -- { serverError 1000 }
