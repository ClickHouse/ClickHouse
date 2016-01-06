@echo off
if defined VS100COMNTOOLS (
call "%VS100COMNTOOLS%\vsvars32.bat")
buildwin 100 build shared both Win32 samples
