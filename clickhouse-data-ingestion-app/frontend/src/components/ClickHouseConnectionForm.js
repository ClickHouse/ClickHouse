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
import { connectToClickHouse } from '../services/api';

const ClickHouseConnectionForm = ({ onConnected }) => {
  const [formData, setFormData] = useState({
    host: 'localhost',
    port: '9000',
    database: '',
    username: 'default',
    password: '',
    useJWT: false,
    jwtToken: '',
    secure: false,
    skipVerify: false,
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
        host: formData.host,
        port: formData.port,
        database: formData.database,
        username: formData.username,
        password: formData.useJWT ? '' : formData.password,
        jwt_token: formData.useJWT ? formData.jwtToken : '',
        use_jwt: formData.useJWT,
        secure: formData.secure,
        skip_verify: formData.skipVerify,
      };

      // Call the API to connect to ClickHouse
      const response = await connectToClickHouse(connectionRequest);

      // Check if connection was successful
      if (response.success) {
        if (onConnected) {
          onConnected({
            connectionId: response.connection_id,
            connectionInfo: {
              host: formData.host,
              database: formData.database,
              username: formData.username,
            },
          });
        }
      } else {
        setError(response.error || 'Failed to connect to ClickHouse');
      }
    } catch (err) {
      setError(err.error || 'An error occurred while connecting to ClickHouse');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        Connect to ClickHouse
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Box component="form" onSubmit={handleSubmit}>
        <Grid container spacing={2}>
          <Grid item xs={12} sm={8}>
            <TextField
              fullWidth
              label="Host"
              name="host"
              value={formData.host}
              onChange={handleChange}
              margin="normal"
              required
            />
          </Grid>
          <Grid item xs={12} sm={4}>
            <TextField
              fullWidth
              label="Port"
              name="port"
              value={formData.port}
              onChange={handleChange}
              margin="normal"
              required
            />
          </Grid>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Database"
              name="database"
              value={formData.database}
              onChange={handleChange}
              margin="normal"
              required
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              fullWidth
              label="Username"
              name="username"
              value={formData.username}
              onChange={handleChange}
              margin="normal"
              required
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            {!formData.useJWT ? (
              <TextField
                fullWidth
                label="Password"
                name="password"
                type="password"
                value={formData.password}
                onChange={handleChange}
                margin="normal"
              />
            ) : (
              <TextField
                fullWidth
                label="JWT Token"
                name="jwtToken"
                value={formData.jwtToken}
                onChange={handleChange}
                margin="normal"
                required
              />
            )}
          </Grid>
          <Grid item xs={12}>
            <FormControlLabel
              control={
                <Switch
                  checked={formData.useJWT}
                  onChange={handleChange}
                  name="useJWT"
                  color="primary"
                />
              }
              label="Use JWT Authentication"
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <FormControlLabel
              control={
                <Switch
                  checked={formData.secure}
                  onChange={handleChange}
                  name="secure"
                  color="primary"
                />
              }
              label="Use TLS/SSL"
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <FormControlLabel
              control={
                <Switch
                  checked={formData.skipVerify}
                  onChange={handleChange}
                  name="skipVerify"
                  color="primary"
                  disabled={!formData.secure}
                />
              }
              label="Skip SSL Verification"
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
          Connect
        </Button>
      </Box>
    </Paper>
  );
};

export default ClickHouseConnectionForm; 