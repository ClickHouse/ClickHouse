@echo off
if defined VS120COMNTOOLS (
call "%VS120COMNTOOLS%\vsvars32.bat")
buildwin 120 build shared both Win32 samples
