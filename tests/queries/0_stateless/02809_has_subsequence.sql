select 'hasSubsequence / const / const';
select hasSubsequence('garbage', '');
select hasSubsequence('garbage', 'g');
select hasSubsequence('garbage', 'a');
select hasSubsequence('garbage', 'e');
select hasSubsequence('garbage', 'gr');
select hasSubsequence('garbage', 'ab');
select hasSubsequence('garbage', 'be');
select hasSubsequence('garbage', 'arg');
select hasSubsequence('garbage', 'garbage');

select hasSubsequence('garbage', 'garbage1');
select hasSubsequence('garbage', 'arbw');
select hasSubsequence('garbage', 'ARG');

select 'hasSubsequence / const / string';
select hasSubsequence('garbage', materialize(''));
select hasSubsequence('garbage', materialize('arg'));
select hasSubsequence('garbage', materialize('arbw'));

select 'hasSubsequence / string / const';
select hasSubsequence(materialize('garbage'), '');
select hasSubsequence(materialize('garbage'), 'arg');
select hasSubsequence(materialize('garbage'), 'arbw');

select 'hasSubsequence / string / string';

select hasSubsequence(materialize('garbage'), materialize(''));
select hasSubsequence(materialize('garbage'), materialize('arg'));
select hasSubsequence(materialize('garbage'), materialize('garbage1'));

select 'hasSubsequenceCaseInsensitive / const / const';

select hasSubsequenceCaseInsensitive('garbage', 'ARG');

