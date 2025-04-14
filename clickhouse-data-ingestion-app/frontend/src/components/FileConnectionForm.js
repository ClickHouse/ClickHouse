import React, { useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import TextField from '@mui/material/TextField';
import Button from '@mui/material/Button';
import FormControlLabel from '@mui/material/FormControlLabel';
import Switch from '@mui/material/Switch';
import Paper from '@mui/material/Paper';
import Grid from '@mui/material/Grid';
import Alert from '@mui/material/Alert';
import LinearProgress from '@mui/material/LinearProgress';
import { configureFileConnection } from '../services/api';

const FileConnectionForm = ({ onConnected }) => {
  const [formData, setFormData] = useState({
    delimiter: ',',
    hasHeader: true,
  });

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData({
      ...formData,
      [name]: type === 'checkbox' ? checked : value,
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      // Prepare the connection request object
      const connectionRequest = {
        delimiter: formData.delimiter,
        has_header: formData.hasHeader,
      };

      // Call the API to configure file connection
      const response = await configureFileConnection(connectionRequest);

      // Check if connection was successful
      if (response.success) {
        if (onConnected) {
          onConnected({
            connectionId: response.connection_id,
            connectionInfo: {
              delimiter: formData.delimiter,
              hasHeader: formData.hasHeader,
            },
          });
        }
      } else {
        setError(response.error || 'Failed to configure file connection');
      }
    } catch (err) {
      setError(err.error || 'An error occurred while configuring file connection');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        Configure File Connection
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Box component="form" onSubmit={handleSubmit}>
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Delimiter"
              name="delimiter"
              value={formData.delimiter}
              onChange={handleChange}
              margin="normal"
              required
              helperText="Character used to separate values in the file (e.g., ',' for CSV, '\\t' for TSV)"
            />
          </Grid>
          <Grid item xs={12}>
            <FormControlLabel
              control={
                <Switch
                  checked={formData.hasHeader}
                  onChange={handleChange}
                  name="hasHeader"
                  color="primary"
                />
              }
              label="File has header row"
            />
          </Grid>
        </Grid>

        {loading && <LinearProgress sx={{ mt: 2 }} />}

        <Button
          type="submit"
          variant="contained"
          disabled={loading}
          sx={{ mt: 3 }}
          fullWidth
        >
          Configure
        </Button>
      </Box>
    </Paper>
  );
};

export default FileConnectionForm; 