import React, { useState, useEffect } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Paper from '@mui/material/Paper';
import LinearProgress from '@mui/material/LinearProgress';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import Button from '@mui/material/Button';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import { getTransferProgress } from '../services/api';

const TransferProgress = ({ transferId, onComplete }) => {
  const [progress, setProgress] = useState({
    processedRecords: 0,
    totalRecords: 0,
    percentage: 0,
    status: 'Initializing...',
  });
  const [error, setError] = useState(null);
  const [isComplete, setIsComplete] = useState(false);
  const [pollingInterval, setPollingInterval] = useState(null);

  useEffect(() => {
    if (transferId) {
      // Start polling for progress updates
      const interval = setInterval(fetchProgress, 1000);
      setPollingInterval(interval);

      return () => {
        clearInterval(interval);
      };
    }
  }, [transferId]);

  const fetchProgress = async () => {
    try {
      const response = await getTransferProgress(transferId);

      if (response.success) {
        if (response.status === 'complete') {
          setIsComplete(true);
          clearInterval(pollingInterval);
          setProgress((prev) => ({
            ...prev,
            status: 'Transfer completed successfully!',
            percentage: 100,
          }));
          if (onComplete) {
            onComplete({
              success: true,
              recordCount: progress.processedRecords,
            });
          }
        } else {
          setProgress({
            processedRecords: response.processed_records || 0,
            totalRecords: response.total_records || 0,
            percentage: response.percentage || 0,
            status: response.status_message || 'Processing...',
          });
        }
      } else {
        setError(response.error || 'Failed to get transfer progress');
      }
    } catch (err) {
      setError(err.error || 'An error occurred while getting transfer progress');
    }
  };

  return (
    <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        Transfer Progress
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Box sx={{ width: '100%', mb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
          <Box sx={{ width: '100%', mr: 1 }}>
            <LinearProgress
              variant="determinate"
              value={progress.percentage}
              color={isComplete ? 'success' : 'primary'}
              sx={{ height: 10, borderRadius: 5 }}
            />
          </Box>
          <Box sx={{ minWidth: 50 }}>
            <Typography variant="body2" color="text.secondary">
              {Math.round(progress.percentage)}%
            </Typography>
          </Box>
        </Box>
        <Typography variant="body2" color="text.secondary">
          {progress.processedRecords.toLocaleString()} of{' '}
          {progress.totalRecords > 0
            ? progress.totalRecords.toLocaleString()
            : 'unknown'}{' '}
          records processed
        </Typography>
      </Box>

      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          p: 2,
          bgcolor: 'background.paper',
          borderRadius: 1,
          border: '1px solid',
          borderColor: 'divider',
        }}
      >
        {isComplete ? (
          <CheckCircleIcon color="success" sx={{ mr: 1 }} />
        ) : (
          <Box sx={{ mr: 1, display: 'inline-block', position: 'relative', width: 24, height: 24 }}>
            <CircularProgress size={24} />
          </Box>
        )}
        <Typography variant="body1">{progress.status}</Typography>
      </Box>

      {isComplete && (
        <Button
          variant="contained"
          color="primary"
          fullWidth
          sx={{ mt: 2 }}
          onClick={() => {
            if (onComplete) {
              onComplete({
                success: true,
                recordCount: progress.processedRecords,
              });
            }
          }}
        >
          View Results
        </Button>
      )}
    </Paper>
  );
};

export default TransferProgress; 