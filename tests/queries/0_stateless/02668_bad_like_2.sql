select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%X\\-Forwarded\\-For%');
select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%X\\-Forwarded\\-For%Cdn%');
select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%X-Forwarded-For%');
select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%X-Forwarded-For%Cdn%');
select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%X\\-Forwarded\\-For: \\(null\\)%\\\nCdn\\-Src\\-Ip: \\(null\\)%\\\nClientip: \\(null\\)%');
select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%\n%');
select like('X-Forwarded-For: (null)\nCdn-Src-Ip: (null)\nClientip: (null)', '%\\\n%');
select 'C:\\Windows\\System32\\cmd.exe' like '%C:\\Windows\\System32\\cmd.exe%';
select 'C:\\Windows\\System32\\Speech_OneCore\\common\\SpeechModelDownload.exe' like '%C:\\Windows\\System32\\Speech_OneCore\\common\\SpeechModelDownload.exe%';