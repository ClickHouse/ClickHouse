SELECT * FROM url('http://127.0.0.1:8123?query=select+{1,2}+as+x+format+TSV', 'TSV') WHERE 0;
SELECT _path FROM url('http://127.0.0.1:8123?query=select+{1,2}+as+x+format+TSV', 'TSV') WHERE 0;

