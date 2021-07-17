SELECT decrypt('aes-128-gcm', 'text', 'key', 'IV'); -- { serverError 36 }
