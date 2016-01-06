@echo off
if defined VS110COMNTOOLS (
call "%VS110COMNTOOLS%\vsvars32.bat")
buildwin 110 build shared both Win32 samples
