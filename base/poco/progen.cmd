@echo off

FOR /R %%f IN (*.progen) DO p:\git\poco-1.9.1\progen\bin\static_mt\progen %%f

