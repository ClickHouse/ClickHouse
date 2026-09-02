SELECT match('Too late', 'Too late');
select match('Too late', '(?i)Too late');
select match('Too late', '(?i)too late');
select match('Too late', '(?i:too late)');
select match('Too late', '(?i)to{2} late');
select match('Too late', '(?i)to(?)o late');
select match('Too late', '(?i)to+ late');
select match('Too late', '(?i)to(?:o|o) late');
