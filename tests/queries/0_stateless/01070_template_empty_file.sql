select 1 format Template settings format_template_row='01070_nonexistent_file.txt'; -- { clientError FILE_DOESNT_EXIST }
select 1 format Template settings format_template_row='/dev/null'; -- { clientError INVALID_TEMPLATE_FORMAT }
