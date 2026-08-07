select initcap('');
select initcap('Hello');
select initcap('hello');
select initcap('hello world');
select initcap('yeah, well, i`m gonna go build my own theme park');
select initcap('CRC32IEEE is the best function');
select initcap('42oK');

select initcapUTF8('');
select initcapUTF8('Hello');
select initcapUTF8('yeah, well, i`m gonna go build my own theme park');
select initcapUTF8('привет, как дела?');
select initcapUTF8('ätsch, bätsch');
select initcapUTF8('We dont support cases when lowercase and uppercase characters occupy different number of bytes in UTF-8. As an example, this happens for ß and ẞ.');