select b'';
select b'0' == '\0';
select b'00110000'; -- 0
select b'0011000100110000'; -- 10
select b'111001101011010110001011111010001010111110010101' == '测试';

select B'';
select B'0' == '\0';
select B'00110000'; -- 0
select B'0011000100110000'; -- 10
select B'111001101011010110001011111010001010111110010101' == '测试';

select x'';
select x'0' == '\0';
select x'30'; -- 0
select x'3130'; -- 10
select x'e6b58be8af95' == '测试';

select X'';
select X'0' == '\0';
select X'30'; -- 0
select X'3130'; -- 10
select X'e6b58be8af95' == '测试';


select x'' == b'';
select x'0' == b'0';
select X'' == X'';
select X'0' == X'0';
