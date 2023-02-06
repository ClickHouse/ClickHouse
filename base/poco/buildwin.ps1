#
# POCO build script
#
# Usage:
# ------
# buildwin.ps1 [-poco_base    dir]
#              [-vs           140 | 150 | 160]
#              [-action       build | rebuild | clean]
#              [-linkmode     shared | static_mt | static_md | all]
#              [-config       release | debug | both]
#              [-platform     Win32 | x64 | WinCE | WEC2013]
#              [-samples]
#              [-tests]
#              [-omit         "Lib1X;LibY;LibZ;..."]
#              [-tool         msbuild | devenv]
#              [-useenv       env | noenv]
#              [-verbosity    m[inimal] | q[uiet] | n[ormal] | d[etailed] | diag[nostic]]
#              [-openssl_base dir]
#              [-mysql_base   dir]

[CmdletBinding()]
Param
(
  [Parameter()]
  [string] $poco_base,

  [Parameter()]
  [ValidateSet(140, 150, 160)]
  [int] $vs = 140,

  [Parameter()]
  [ValidateSet('build', 'rebuild', 'clean')]
  [string] $action = 'build',

  [Parameter()]
  [ValidateSet('shared', 'static_mt', 'static_md', 'all')]
  [string] $linkmode = 'shared',

  [Parameter()]
  [ValidateSet('release', 'debug', 'both')]
  [string] $config = 'release',

  [Parameter()]
  [ValidateSet('Win32', 'x64', 'WinCE', 'WEC2013')]
  [string] $platform = 'x64',

  [switch] $tests = $false,
  [switch] $samples = $false,
  [string] $omit,

  [Parameter()]
  [ValidateSet('msbuild', 'devenv')]
  [string] $tool = 'msbuild',

  [Parameter()]
  [ValidateSet('env', 'noenv')]
  [string] $useenv = 'env',

  [Parameter()]
  [ValidateSet('quiet', 'm[inimal]', 'n[ormal]', 'd[etailed]', 'diag[nostic]')]
  [string] $verbosity = 'minimal',

  [Parameter()]
  [string] $openssl_base,

  [Parameter()]
  [string] $mysql_base,

  [switch] $help
)

function Add-VSCOMNTOOLS([int] $vsver)
{
  if ($vsver -ge 150)
	{
		$vssetup= $([Environment]::GetFolderPath("MyDocuments"))
		$vssetup= Join-Path $vssetup "WindowsPowerShell"
		$vssetup= Join-Path $vssetup "Modules"
		$vssetup= Join-Path $vssetup "VSSetup"
		if (-not (Test-Path $vssetup))
		{
			Install-Module VSSetup -Scope CurrentUser -Force
		}
		if ($vsver -eq 150)
		{
			$range='[15.0,16.0)'
		}
		if ($vsver -eq 160)
		{
			$range='[16.0,17.0)'
		}
		
		$installationPath = Get-VSSetupInstance | Select-VSSetupInstance -Version $range -Latest -Require Microsoft.VisualStudio.Component.VC.Tools.x86.x64 | select InstallationPath
		$vscomntools = $installationPath.psobject.properties.Value;
		if ($vsver -eq 150)
		{
			set-item -force -path "ENV:VS150COMNTOOLS"  -value "$vscomntools\Common7\Tools\"
			Write-Host "`nVS150COMNTOOLS=$env:VS150COMNTOOLS" -ForegroundColor Yellow
		}
		if ($vsver -eq 160)
		{
			set-item -force -path "ENV:VS160COMNTOOLS"  -value "$vscomntools\Common7\Tools\"
			Write-Host "`nVS160COMNTOOLS=$env:VS160COMNTOOLS" -ForegroundColor Yellow
		}
		
	}
}

function Add-Env-Var([string] $lib, [string] $var)
{
  if ((${Env:$var} -eq $null) -or (-not ${Env:$var}.Contains(${Env:$lib_$var"})))
  {
    $libvar = "$lib" + "_" + "$var"
    $envvar = [Environment]::GetEnvironmentVariable($var, "Process")
    $envvar = $envvar + ';' + [Environment]::GetEnvironmentVariable($libvar, "Process")
    [Environment]::SetEnvironmentVariable($var, $envvar, "Process")
  }
}


function Set-Environment
{
  if ($poco_base -eq '') { $script:poco_base = Get-Location }

  switch ( $vs )
  {
    140 { }
	default { Add-VSCOMNTOOLS $vs }
  }

  if (-Not $Env:PATH.Contains("$Env:POCO_BASE\bin64;$Env:POCO_BASE\bin;"))
  { $Env:PATH = "$Env:POCO_BASE\bin64;$Env:POCO_BASE\bin;$Env:PATH" }

  if ($openssl_base -eq '')
  {
    $script:openssl_base = '$poco_base\openssl'
  }

  $Env:OPENSSL_DIR     = "$openssl_base"
  $Env:OPENSSL_INCLUDE = "$Env:OPENSSL_DIR\include"
  $Env:OPENSSL_LIB     = "$Env:OPENSSL_DIR\lib;$Env:OPENSSL_DIR\lib\VC"
  Add-Env-Var "OPENSSL" "INCLUDE"
  Add-Env-Var "OPENSSL" "LIB"

  if ($mysql_base -ne '')
  {
    $Env:MYSQL_DIR     = "$mysql_base"
    $Env:MYSQL_INCLUDE = "$Env:MYSQL_DIR\include"
    $Env:MYSQL_LIB     = "$Env:MYSQL_DIR\lib"
    Add-Env-Var "MYSQL" "INCLUDE"
    Add-Env-Var "MYSQL" "LIB"
  }

  $vsct = "VS$($vs)COMNTOOLS"
  $vsdir = ''
  $vsdir = (Get-Item Env:$vsct).Value
  $Command = ''
  $CommandArg = ''
  if ($platform -eq 'x64') { $CommandArg = "amd64" }
  else                     { $CommandArg = "x86" }
  if ($vs -eq 150)
  {
    $Command = Resolve-Path "$($vsdir)\..\..\VC\Auxiliary\Build\vcvarsall.bat"
    $script:msbuild_exe = Resolve-Path "$($vsdir)\..\..\MSBuild\15.0\Bin\MSBuild.exe"
  } else {
  if ($vs -eq 160)
  {
    $Command = Resolve-Path "$($vsdir)\..\..\VC\Auxiliary\Build\vcvarsall.bat"
    $script:msbuild_exe = Resolve-Path "$($vsdir)\..\..\MSBuild\Current\Bin\MSBuild.exe"
  }
  else
  {
    $Command = Resolve-Path "$($vsdir)\..\..\VC\vcvarsall.bat"
    $script:msbuild_exe = "MSBuild.exe"
  }}

  $tempFile = [IO.Path]::GetTempFileName()
  cmd /c " `"$Command`" $CommandArg && set > `"$tempFile`" "
  Get-Content $tempFile | Foreach-Object {
    if($_ -match "^(.*?)=(.*)$")
    {
      Set-Content "Env:$($matches[1])" $matches[2]
    }
  }
  Remove-Item $tempFile
}


function Process-Input
{
  if ($help -eq $true)
  {
    Write-Host 'Usage:'
    Write-Host '------'
    Write-Host 'buildwin.ps1 [-poco_base    <dir>]'
    Write-Host '             [-vs           140 | 150 | 160]'
    Write-Host '             [-action       build | rebuild | clean]'
    Write-Host '             [-linkmode     shared | static_mt | static_md | all]'
    Write-Host '             [-config       release | debug | both]'
    Write-Host '             [-platform     Win32 | x64 | WinCE | WEC2013]'
    Write-Host '             [-samples]'
    Write-Host '             [-tests]'
    Write-Host '             [-omit         "Lib1X;LibY;LibZ;..."]'
    Write-Host '             [-tool         msbuild | devenv]'
    Write-Host '             [-useenv       env | noenv]'
    Write-Host '             [-verbosity    minimal | quiet | normal | detailed | diagnostic'
    Write-Host '             [-openssl_base <dir>]'
    Write-Host '             [-mysql_base   <dir>]'

    Exit
  }
  else
  {
    Set-Environment

    Write-Host "Build configuration:"
    Write-Host "--------------------"
    Write-Host "Poco Base:     $poco_base"
    Write-Host "Version:       $vs"
    Write-Host "Action:        $action"
    Write-Host "Link Mode:     $linkmode"
    Write-Host "Configuration: $config"
    Write-Host "Platform:      $platform"
    Write-Host "Tests:         $tests"
    Write-Host "Samples:       $samples"
    Write-Host "Build Tool:    $tool"

    if ($omit -ne '')
    {
      Write-Host "Omit:          $omit"
    }

    if ($openssl_base -ne '')
    {
      Write-Host "OpenSSL:       $openssl_base"
    }

    if ($mysql_base -ne '')
    {
      Write-Host "MySQL:         $mysql_base"
    }

    # NB: this won't work in PowerShell ISE
    #Write-Host "Press Ctrl-C to exit or any other key to continue ..."
    #$x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyUp")
  }
}


function Exec-MSBuild([string] $vsProject, [string] $projectConfig)
{
  if (!(Test-Path -Path $vsProject -PathType leaf)) {
    Write-Host "Project $vsProject not found, skipping."
    return
  }

  $cmd = "&`"$script:msbuild_exe`" $vsProject /nologo /m /t:$action /p:Configuration=$projectConfig /p:BuildProjectReferences=false /p:Platform=$platform /p:useenv=$useenv /v:$verbosity"
  Write-Host $cmd
  Invoke-Expression $cmd
  if ($LastExitCode -ne 0) { Exit $LastExitCode }
}


function Build-MSBuild([string] $vsProject, [switch] $skipStatic)
{
  if ($linkmode -contains "static" -and $skipStatic) { Return }

  if ($linkmode -eq 'all')
  {
    $linkModeArr = @('shared')
    if (-not $skipStatic)
    {
      $linkModeArr += 'static_mt', 'static_md'
    }

    foreach ($mode in $linkModeArr)
    {
      if ($config -eq 'both')
      {
        $configArr = 'release', 'debug'
        foreach ($cfg in $configArr)
        {
          Exec-MSBuild $vsProject "$($cfg)_$($mode)"
        }
      }
      else #config
      {
        Exec-MSBuild $vsProject "$($config)_$($mode)"
      }
    }
  }
  else #linkmode
  {
    if ($config -eq 'both')
    {
      $configArr = 'release', 'debug'
      foreach ($cfg in $configArr)
      {
        Exec-MSBuild $vsProject "$($cfg)_$($linkmode)"
      }
    }
    else #config
    {
      Exec-MSBuild $vsProject "$($config)_$($linkmode)"
    }
  }
}


function Exec-Devenv([string] $projectConfig, [string] $vsProject)
{
  $cmd = "devenv /useenv /$action $projectConfig $vsProject"
  Write-Host $cmd
  Invoke-Expression $cmd
}


function Build-Devenv([string] $vsProject, [switch] $skipStatic)
{
  if ($linkmode -contains "static" -and $skipStatic) { Return }

  if ($linkmode -eq 'all')
  {
    $linkModeArr = @('shared')
    if (-not $skipStatic)
    {
      $linkModeArr += 'static_mt', 'static_md'
    }

    foreach ($mode in $linkModeArr)
    {
      if ($config -eq 'both')
      {
        $configArr = 'release', 'debug'
        foreach ($cfg in $configArr)
        {
          Exec-Devenv "$($cfg)_$($mode)" $vsProject
        }
      }
      else #config
      {
        Exec-Devenv "$($config)_$($mode)" $vsProject
      }
    }
  }
  else #linkmode
  {
    if ($config -eq 'both')
    {
      $configArr = 'release', 'debug'
      foreach ($cfg in $configArr)
      {
        Exec-Devenv "$($cfg)_$($linkmode)" $vsProject
      }
    }
    else #config
    {
      Exec-Devenv "$($config)_$($linkmode)" $vsProject
    }
  }
}


function Build-samples
{
  process {
    $sampleName = $_.BaseName.split("_")[0]
    $sampleProjName = "$($poco_base)\$($componentDir)\samples\$($sampleName)\$($_)"
    if ($tool -eq 'devenv') { Build-Devenv $sampleProjName }
    elseif ($tool -eq 'msbuild') { Build-MSBuild $sampleProjName }
    else{ Write-Host "Tool not supported: $tool" }
  }
}


function Build-Exec([string] $tool, [string] $vsProject, [switch] $skipStatic)
{
   if (!(Test-Path -Path $vsProject)) # not found
   {
      Write-Host "+------------------------------------------------------------------"
      Write-Host "| VS project $vsProject not found, skipping."
      Write-Host "+------------------------------------------------------------------"
      Return
   }
   if     ($tool -eq 'devenv')  { Build-Devenv $vsProject -skipStatic:$skipStatic }
   elseif ($tool -eq 'msbuild') { Build-MSBuild $vsProject -skipStatic:$skipStatic }
   else
   {
      Write-Host "Build tool $tool not supported. Exiting."
      Exit -1
   }
}


function Build-Components([string] $extension, [string] $platformName, [string] $type)
{

  Get-Content "$poco_base\components" | Foreach-Object {

    $component = $_
    $componentDir = $_.Replace("/", "\")
    $componentArr = $_.split('/')
    $componentName = $componentArr[$componentArr.Length - 1]
    $suffix = "_vs$vs"

    $omitArray = @()
    $omit.Split(',;') | ForEach {
        $omitArray += $_.Trim()
    }

    if ($omitArray -NotContains $component)
    {
      $vsProject = "$poco_base\$componentDir\$componentName$($platformName)$($suffix).$($extension)"

      if (!(Test-Path -Path $vsProject)) # when VS project name is not same as directory name
      {
        $vsProject = "$poco_base\$componentDir$($platformName)$($suffix).$($extension)"
        if (!(Test-Path -Path $vsProject)) # not found
        {
          Write-Host "+------------------------------------------------------------------"
          Write-Host "| VS project $vsProject not found, skipping."
          Write-Host "+------------------------------------------------------------------"
          Return # since Foreach-Object is a function, this is actually loop "continue"
        }
      }

      Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
      Write-Host "| Building $vsProject"
      Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

      if ($type -eq "lib")
      {
        Build-Exec $tool $vsProject
      }
      ElseIf ($tests -and ($type -eq "test"))
      {
        $vsTestProject = "$poco_base\$componentDir\testsuite\TestSuite$($platformName)$($suffix).$($extension)"
        Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        Write-Host "| Building $vsTestProject"
        Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        Build-Exec $tool $vsTestProject

        if ($component -eq "Foundation") # special case for Foundation, which needs test app and dll
        {
          $vsTestProject = "$poco_base\$componentDir\testsuite\TestApp$($platformName)$($suffix).$($extension)"
          Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
          Write-Host "| Building $vsTestProject"
          Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
          Build-Exec $tool $vsTestProject

          $vsTestProject = "$poco_base\$componentDir\testsuite\TestLibrary$($platformName)$($suffix).$($extension)"
          Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
          Write-Host "| Building $vsTestProject"
          Write-Host "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
          Build-Exec $tool $vsTestProject -skipStatic
        }
      }
      ElseIf ($samples -and ($type -eq "sample"))
      {
        if ($platform -eq 'x64')
        {
          Get-Childitem "$poco_base\$($componentDir)" -Recurse |`
            Where {$_.Extension -Match $extension -And $_.DirectoryName -Like "*samples*" -And $_.BaseName -Like "*$platformName$($suffix)" } `
            | Build-samples "$_"
        }
        else
        {
          Get-Childitem "$poco_base\$($componentDir)" -Recurse |`
            Where {$_.Extension -Match $extension -And $_.DirectoryName -Like "*samples*" -And $_.BaseName -Like "*$($suffix)" -And $_.BaseName -NotLike "*_x64_*" } `
            | Build-samples "$_"
        }
      }
    }
    else
    {
      Write-Host "-------------------------------"
      Write-Host "# Skipping $componentDir"
      Write-Host "-------------------------------"
    }
  }
}


function Build
{
  Process-Input

  if ($vs -lt 100) { $extension = 'vcproj'  }
  else                     { $extension = 'vcxproj' }

  $platformName = ''
  if ($platform -eq 'x64')       { $platformName = '_x64' }
  elseif ($platform -eq 'WinCE') { $platformName = '_CE' }

  Build-Components $extension $platformName "lib"
  Build-Components $extension $platformName "test"
  Build-Components $extension $platformName "sample"
}


Build
