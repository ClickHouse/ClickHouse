SELECT translate('aaa'::FixedString(10), 'a','A');
SELECT translate(number::String::FixedString(1), '0','A') from numbers(4);
