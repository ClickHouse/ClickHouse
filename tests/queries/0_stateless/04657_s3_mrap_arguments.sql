SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', format='CSV', structure='x UInt8'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('arn:aws:s3::123456789012:accesspoint/example.mrap', key='', format='CSV', structure='x UInt8'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('arn:aws:s3:us-east-1:123456789012:accesspoint/example.mrap', key='key', format='CSV', structure='x UInt8'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('https://bucket.s3.amazonaws.com/key', key='another-key', format='CSV', structure='x UInt8'); -- { serverError BAD_ARGUMENTS }

CREATE NAMED COLLECTION mrap_invalid_arguments AS mrap_arn='arn:aws:s3::123456789012:accesspoint/example.mrap', key='', format='CSV', structure='x UInt8';
SELECT * FROM s3(mrap_invalid_arguments); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION mrap_invalid_arguments;
