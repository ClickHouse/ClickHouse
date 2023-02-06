#! /bin/bash

COMPILER=Compiler/bin/Darwin/x86_64/tec

WINDOWS_CODEPAGES="874 932 936 949 950 1253 1254 1255 1256 1257 1258"

for cp in $WINDOWS_CODEPAGES ;
do
	echo Generating encoding for Windows Code Page ${cp}...
	$COMPILER http://www.unicode.org/Public/MAPPINGS/VENDORS/MICSFT/WINDOWS/CP${cp}.TXT -c Windows${cp}Encoding -e windows-${cp} -e Windows-${cp} -e cp${cp} -e CP${cp}
done

$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-3.TXT -c ISO8859_3Encoding -e ISO-8859-3 -e Latin3 -e Latin-3
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-4.TXT -c ISO8859_4Encoding -e ISO-8859-4 -e Latin4 -e Latin-4
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-5.TXT -c ISO8859_5Encoding -e ISO-8859-5
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-6.TXT -c ISO8859_6Encoding -e ISO-8859-6
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-7.TXT -c ISO8859_7Encoding -e ISO-8859-7
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-8.TXT -c ISO8859_8Encoding -e ISO-8859-8
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-9.TXT -c ISO8859_9Encoding -e ISO-8859-9 -e Latin5 -e Latin-5
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-10.TXT -c ISO8859_10Encoding -e ISO-8859-10 -e Latin6 -e Latin-6
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-11.TXT -c ISO8859_11Encoding -e ISO-8859-11
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-13.TXT -c ISO8859_13Encoding -e ISO-8859-13 -e Latin7 -e Latin-7
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-14.TXT -c ISO8859_14Encoding -e ISO-8859-14 -e Latin8 -e Latin-8
$COMPILER http://www.unicode.org/Public/MAPPINGS/ISO8859/8859-16.TXT -c ISO8859_16Encoding -e ISO-8859-16 -e Latin10 -e Latin-10

