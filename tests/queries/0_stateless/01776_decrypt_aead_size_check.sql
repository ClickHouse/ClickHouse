-- this line is to bump CI in version 21.1
SELECT decrypt('aes-128-gcm', 'text', 'key', 'IV'); -- { serverError 36 }
