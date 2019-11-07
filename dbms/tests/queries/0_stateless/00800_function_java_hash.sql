select javaHash('abc');
select javaHash('874293087');
select javaHashUTF16LE(convertCharset('a1가', 'utf-8', 'utf-16le'));
select javaHashUTF16LE(convertCharset('가나다라마바사아자차카타파하', 'utf-8', 'utf-16le'));
select javaHashUTF16LE(convertCharset('FJKLDSJFIOLD_389159837589429', 'utf-8', 'utf-16le'));
select hiveHash('abc');
select hiveHash('874293087');
