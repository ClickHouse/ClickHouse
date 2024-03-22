/** NOTE: The behaviour of substring and substringUTF8 is inconsistent when negative offset is greater than string size:
  * substring:
  *      hello
  * ^-----^ - offset -10, length 7, result: "he"
  * substringUTF8:
  *      hello
  *      ^-----^ - offset -10, length 7, result: "hello"
  * This may be subject for change.
  */
SELECT substringUTF8('hello, Ð¿Ñ�Ð¸Ð²ÐµÑ�', -9223372036854775808, number) FROM numbers(16) FORMAT Null;
