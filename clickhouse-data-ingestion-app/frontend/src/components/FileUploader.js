import React, { useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';
import Paper from '@mui/material/Paper';
import Alert from '@mui/material/Alert';
import LinearProgress from '@mui/material/LinearProgress';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import DescriptionIcon from '@mui/icons-material/Description';
import ClearIcon from '@mui/icons-material/Clear';
import IconButton from '@mui/material/IconButton';

const FileUploader = ({ onFileSelected, acceptedTypes = '.csv,.txt,.tsv,.dat' }) => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      setError(null);
      
      // Check if file type is accepted
      const fileExtension = file.name.split('.').pop().toLowerCase();
      const isAccepted = acceptedTypes
        .split(',')
        .some(type => type.includes(fileExtension));
      
      if (!isAccepted) {
        setError(`File type not supported. Please select one of: ${acceptedTypes}`);
        return;
      }
      
      setSelectedFile(file);
      
      if (onFileSelected) {
        onFileSelected(file);
      }
    }
  };

  const handleDragOver = (event) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const handleDrop = (event) => {
    event.preventDefault();
    event.stopPropagation();
    
    const file = event.dataTransfer.files[0];
    if (file) {
      // Check if file type is accepted
      const fileExtension = file.name.split('.').pop().toLowerCase();
      const isAccepted = acceptedTypes
        .split(',')
        .some(type => type.includes(fileExtension));
      
      if (!isAccepted) {
        setError(`File type not supported. Please select one of: ${acceptedTypes}`);
        return;
      }
      
      setSelectedFile(file);
      
      if (onFileSelected) {
        onFileSelected(file);
      }
    }
  };

  const clearFile = () => {
    setSelectedFile(null);
    setError(null);
    
    if (onFileSelected) {
      onFileSelected(null);
    }
  };

  return (
    <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        Select File
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {selectedFile ? (
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            p: 2,
            border: '1px solid',
            borderColor: 'primary.main',
            borderRadius: 1,
            bgcolor: 'background.paper',
          }}
        >
          <DescriptionIcon color="primary" sx={{ mr: 2 }} />
          <Box sx={{ flexGrow: 1 }}>
            <Typography variant="subtitle1" noWrap>
              {selectedFile.name}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
            </Typography>
          </Box>
          <IconButton onClick={clearFile} aria-label="clear file">
            <ClearIcon />
          </IconButton>
        </Box>
      ) : (
        <Box
          onDragOver={handleDragOver}
          onDrop={handleDrop}
          component="label"
          htmlFor="file-upload"
          sx={{
            border: '2px dashed',
            borderColor: 'divider',
            borderRadius: 1,
            p: 3,
            textAlign: 'center',
            cursor: 'pointer',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            '&:hover': {
              borderColor: 'primary.main',
              bgcolor: 'action.hover',
            },
          }}
        >
          <input
            id="file-upload"
            type="file"
            onChange={handleFileChange}
            style={{ display: 'none' }}
            accept={acceptedTypes}
          />
          <UploadFileIcon color="primary" sx={{ fontSize: 48, mb: 1 }} />
          <Typography variant="subtitle1" gutterBottom>
            Drag & drop your file here
          </Typography>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            or
          </Typography>
          <Button variant="outlined" component="span">
            Browse Files
          </Button>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
            Supported file types: {acceptedTypes}
          </Typography>
        </Box>
      )}

      {loading && <LinearProgress sx={{ mt: 2 }} />}
    </Paper>
  );
};

export default FileUploader; 