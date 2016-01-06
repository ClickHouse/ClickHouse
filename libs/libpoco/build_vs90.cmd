@echo off
if defined VS90COMNTOOLS (
call "%VS90COMNTOOLS%\vsvars32.bat")
buildwin 90 build shared both Win32 samples
